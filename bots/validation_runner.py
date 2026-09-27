"""Run catalogued validation recipes without executing candidate-selected commands.

The control plane leases a gate before this runner sees it.  Every executable is
selected by a root-installed operator catalog; gate arguments are assertions,
not command input.  Candidate source is an immutable, exact-head artifact
unpacked into a private read-only tree.
"""
from __future__ import annotations

import gzip
import importlib.util
import selectors
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import tarfile
import tempfile
import time
import uuid
from contextlib import contextmanager
from functools import cache
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Iterator, Protocol
from urllib.parse import urlsplit

from airflow.providers.vintage.bot_dashboard.validation_recipes import (
    RecipeRequest,
    ValidationRecipeError,
    parse_recipe,
)

_MAX_OUTPUT = 1_048_576
_MAX_NDJSON_LINE = 65_536
_MAX_EXTRACTED_BYTES = 64 * 1024 * 1024
_MAX_EXTRACTED_FILES = 10_000
_TOKEN = re.compile(r"^[a-z][a-z0-9_-]{0,99}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,99}$")
_FORBIDDEN_ENV = re.compile(r"^(?:AIRFLOW__|BOT_DASHBOARD_|BOT_GIT_|GIT_|AWS_|GOOGLE_|AZURE_)")


class ValidationRunnerError(RuntimeError):
    pass


class ValidationCapabilityUnavailable(ValidationRunnerError):
    """The deployed validation isolation/egress capability is not operational."""


class ValidationSourceCheckFailed(ValidationRunnerError):
    """The fixed public-source comparison did not meet its asserted coverage."""


class ValidationSourceUnavailable(ValidationRunnerError):
    """The source check timed out; it cannot be treated as validated."""


@dataclass(frozen=True)
class CommandSpec:
    command_id: str
    recipe: str
    capability: str
    argv: tuple[str, ...]
    timeout_seconds: int
    credential_env: tuple[str, ...] = ()
    network_profile: str | None = None
    source_url: str | None = None
    expected_status: int | None = None
    summary_required: bool = False
    baseline_count: int | None = None

    @classmethod
    def parse(cls, command_id: str, value: object) -> "CommandSpec":
        if not _TOKEN.fullmatch(command_id) or not isinstance(value, dict):
            raise ValidationRunnerError("validation command catalog is invalid")
        expected = {"recipe", "capability", "argv", "timeout_seconds", "credential_env", "network_profile", "source_url", "expected_status", "summary_required", "baseline_count"}
        if set(value) - expected or {"recipe", "capability", "argv", "timeout_seconds"} - set(value):
            raise ValidationRunnerError("validation command catalog has unknown fields")
        recipe, capability, argv, timeout = value["recipe"], value["capability"], value["argv"], value["timeout_seconds"]
        if not isinstance(recipe, str) or not isinstance(capability, str) or not _TOKEN.fullmatch(recipe) or not _TOKEN.fullmatch(capability):
            raise ValidationRunnerError("validation command catalog has invalid identifiers")
        if (not isinstance(argv, list) or not 1 <= len(argv) <= 128 or any(
                not isinstance(part, str) or len(part) > 16384 or "\x00" in part for part in argv)):
            raise ValidationRunnerError("validation command catalog has invalid argv")
        # The operator chooses an immutable executable, not a candidate-relative wrapper.
        executable = Path(argv[0])
        if not executable.is_absolute() or ".." in executable.parts:
            raise ValidationRunnerError("validation command executable must be absolute")
        if type(timeout) is not int or not 1 <= timeout <= 300:
            raise ValidationRunnerError("validation command timeout is outside bounds")
        names = value.get("credential_env", [])
        if not isinstance(names, list) or len(names) > 20 or any(
                not isinstance(name, str) or not _ENV_NAME.fullmatch(name) or _FORBIDDEN_ENV.match(name) for name in names):
            raise ValidationRunnerError("validation credential environment is invalid")
        if len(names) != len(set(names)):
            raise ValidationRunnerError("validation credential environment has duplicates")
        profile = value.get("network_profile")
        if profile is not None and profile != "public-egress-proxy-v1":
            raise ValidationRunnerError("validation network profile is invalid")
        source_url, expected_status = value.get("source_url"), value.get("expected_status")
        if recipe in {"public_source_smoke", "public_source_reconciliation"}:
            if not isinstance(source_url, str) or not source_url.startswith("https://") or type(expected_status) is not int:
                raise ValidationRunnerError("public validation catalog assertions are invalid")
        elif source_url is not None or expected_status is not None:
            raise ValidationRunnerError("non-public validation command has public assertions")
        baseline = value.get("baseline_count")
        if recipe == "public_source_reconciliation":
            if type(baseline) is not int or not 1 <= baseline <= 100 or profile != "public-egress-proxy-v1":
                raise ValidationRunnerError("public reconciliation baseline is invalid")
        elif baseline is not None:
            raise ValidationRunnerError("non-reconciliation command has a coverage baseline")
        summary_required = value.get("summary_required", False)
        if type(summary_required) is not bool or (
                summary_required and recipe not in {"public_source_smoke", "public_source_reconciliation"}):
            raise ValidationRunnerError("validation summary requirement is invalid")
        return cls(command_id, recipe, capability, tuple(argv), timeout, tuple(names), profile, source_url, expected_status, summary_required, baseline)


class CommandCatalog:
    """Validated operator catalog.  It cannot interpolate any gate value into argv."""
    def __init__(self, commands: dict[str, CommandSpec]):
        self._commands = commands

    @classmethod
    def parse(cls, value: object) -> "CommandCatalog":
        if not isinstance(value, dict) or not value or len(value) > 100:
            raise ValidationRunnerError("validation command catalog is invalid")
        commands = {key: CommandSpec.parse(key, spec) for key, spec in value.items()}
        return cls(commands)
    def resolve(self, request: RecipeRequest) -> CommandSpec:
        spec = self._commands.get(request.command_id)
        if spec is None or spec.recipe != request.recipe or spec.capability != request.required_capability:
            raise ValidationRunnerError("validation recipe has no matching trusted command")
        if request.recipe in {"public_source_smoke", "public_source_reconciliation"} and (
                spec.source_url != request.assertions["source_url"]
                or spec.expected_status != request.assertions["expected_status"]):
            raise ValidationRunnerError("validation recipe does not match trusted public source assertions")
        return spec


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    output: bytes
    timed_out: bool
    duration_ms: int
    stderr: bytes = b""


class RestrictedExecutor(Protocol):
    @property
    def capabilities(self) -> set[str]: ...
    def run(self, command: CommandSpec, candidate_root: Path, environment: dict[str, str]) -> CommandResult: ...


class SandboxExecutor:
    """Invoke a root-owned validation sandbox; it receives no dashboard or Git credential."""
    def __init__(self, launcher: Path, capabilities: set[str]):
        self.launcher = launcher
        self._capabilities = frozenset(capabilities)

    @property
    def capabilities(self) -> set[str]:
        return set(self._capabilities)

    def _trusted_launcher(self) -> None:
        try:
            metadata = self.launcher.lstat()
        except OSError as exc:
            raise ValidationCapabilityUnavailable("validation sandbox launcher is unavailable") from exc
        if (not self.launcher.is_absolute() or not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0
                or metadata.st_mode & 0o022 or not os.access(self.launcher, os.X_OK)):
            raise ValidationRunnerError("validation sandbox launcher is unsafe")

    def run(self, command: CommandSpec, candidate_root: Path, environment: dict[str, str]) -> CommandResult:
        self._trusted_launcher()
        with tempfile.TemporaryDirectory(prefix="validation-output-") as temporary:
            output = Path(temporary) / "result.ndjson"
            # This is the fixed validation-launcher protocol.  It must bind the
            # candidate read-only, clear inherited env, limit egress to the named
            # capability, and write only this bounded output path.
            argv = [str(self.launcher), "--protocol", "validation-v1", "--workdir", str(candidate_root),
                    "--output", str(output), "--timeout-seconds", str(command.timeout_seconds),
                    "--capability", command.capability, "--", *command.argv]
            started = time.monotonic()
            env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", **environment}
            try:
                process = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                         stderr=subprocess.DEVNULL, env=env, timeout=command.timeout_seconds + 15,
                                         check=False)
            except subprocess.TimeoutExpired:
                return CommandResult(-1, b"", True, int((time.monotonic() - started) * 1000))
            try:
                data = output.read_bytes()
            except OSError as exc:
                raise ValidationRunnerError("validation sandbox result is unavailable") from exc
            if len(data) > _MAX_OUTPUT:
                raise ValidationRunnerError("validation sandbox result exceeds its output bound")
            return CommandResult(process.returncode, data, False, int((time.monotonic() - started) * 1000))

class CatalogBwrapExecutor:
    """Execute fixed bwrap commands within bounded systemd user cgroups."""
    def __init__(self, capabilities: set[str]):
        self._capabilities = frozenset(capabilities)

    @property
    def capabilities(self) -> set[str]:
        return set(self._capabilities)

    def run(self, command: CommandSpec, candidate_root: Path, environment: dict[str, str]) -> CommandResult:
        try:
            info = Path(command.argv[0]).lstat()
        except OSError as exc:
            raise ValidationCapabilityUnavailable("validation sandbox executable is unavailable") from exc
        if (Path(command.argv[0]) != Path("/usr/bin/bwrap") or not stat.S_ISREG(info.st_mode)
                or info.st_uid != 0 or info.st_mode & 0o022):
            raise ValidationRunnerError("validation catalog executable is unsafe")
        argv = tuple(str(candidate_root) if value == "{candidate_root}" else value for value in command.argv)
        required = {"--unshare-all", "--new-session", "--die-with-parent", "--clearenv"}
        if not required <= set(argv):
            raise ValidationRunnerError("validation catalog confinement is incomplete")
        binds = [(argv[index + 1], argv[index + 2]) for index, value in enumerate(argv[:-2])
                 if value == "--ro-bind"]
        if (str(candidate_root), "/work") not in binds or "--chdir" not in argv:
            raise ValidationRunnerError("validation catalog does not bind the candidate read-only")
        if command.network_profile is None:
            return self._run(argv, candidate_root, environment, command.timeout_seconds)
        if command.network_profile != "public-egress-proxy-v1":
            raise ValidationRunnerError("validation network profile is unsupported")
        try:
            from validation_network import PublicEgressError, public_egress
        except ImportError as exc:
            raise ValidationCapabilityUnavailable("restricted public egress proxy is unavailable") from exc
        try:
            with public_egress(candidate_root, command.timeout_seconds,
                               allowed_hosts={urlsplit(command.source_url).hostname}) as relay:
                socket = relay.socket_path.lstat()
                relay_file = relay.relay_path.lstat()
                if (not stat.S_ISSOCK(socket.st_mode) or socket.st_uid != os.getuid() or socket.st_mode & 0o077
                        or not stat.S_ISREG(relay_file.st_mode) or relay_file.st_uid not in {0, os.getuid()}
                        or relay_file.st_mode & 0o022):
                    raise ValidationRunnerError("public egress relay is unsafe")
                split = argv.index("--")
                argv = (*argv[:split], "--ro-bind", str(relay.socket_path), "/public-egress.sock",
                        "--ro-bind", str(relay.relay_path), "/opt/validation-relay.py",
                        "--setenv", "HTTP_PROXY", relay.proxy_url, "--setenv", "HTTPS_PROXY", relay.proxy_url,
                        "--setenv", "NO_PROXY", "", "--", *relay.argv_prefix, *argv[split + 1:])
                return self._run(argv, candidate_root, environment, command.timeout_seconds)
        except (OSError, PublicEgressError) as exc:
            raise ValidationCapabilityUnavailable("restricted public egress proxy is unavailable") from exc

    @staticmethod
    def _run(argv: tuple[str, ...], candidate_root: Path, environment: dict[str, str], timeout_seconds: int) -> CommandResult:
        started = time.monotonic()
        argv = (
            "/usr/bin/systemd-run", "--user", "--quiet", "--wait", "--pipe", "--collect",
            "--unit=vintage-validation-" + uuid.uuid4().hex,
            "-p", "MemoryMax=512M", "-p", "TasksMax=32", "-p", "CPUQuota=100%",
            "-p", f"RuntimeMaxSec={timeout_seconds}", "--", *argv,
        )
        try:
            process = subprocess.Popen(argv, cwd=candidate_root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, start_new_session=True,
                                       env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
                                            "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"),
                                            "PYTHONDONTWRITEBYTECODE": "1", **environment})
        except OSError as exc:
            raise ValidationCapabilityUnavailable("validation sandbox could not start") from exc
        output = bytearray()
        errors = bytearray()
        timed_out = False
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, (output, _MAX_OUTPUT))
        selector.register(process.stderr, selectors.EVENT_READ, (errors, 131_072))
        try:
            while selector.get_map():
                remaining = timeout_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    timed_out = True
                    break
                for key, _ in selector.select(min(remaining, 0.25)):
                    buffer, bound = key.data
                    chunk = os.read(key.fileobj.fileno(), min(65_536, bound + 1 - len(buffer)))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffer.extend(chunk)
                    if len(buffer) > bound:
                        raise ValidationRunnerError("validation command output exceeds its bound")
            if not timed_out:
                try:
                    process.wait(timeout=max(0.001, timeout_seconds - (time.monotonic() - started)))
                except subprocess.TimeoutExpired:
                    timed_out = True
            return CommandResult(-1 if timed_out else process.returncode, bytes(output), timed_out,
                                 int((time.monotonic() - started) * 1000), bytes(errors))
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            selector.close()
            process.stdout.close()
            process.stderr.close()


class CredentialBroker(Protocol):
    def environment(self, capability: str, names: tuple[str, ...]) -> dict[str, str]: ...


class StaticCredentialBroker:
    """Dedicated validation credentials supplied by the trusted worker deployment."""
    def __init__(self, values: dict[str, dict[str, str]]):
        self.values = values

    def environment(self, capability: str, names: tuple[str, ...]) -> dict[str, str]:
        if not names:
            return {}
        source = self.values.get(capability)
        if source is None:
            raise ValidationRunnerError("validation capability credentials are unavailable")
        result = {name: source[name] for name in names if name in source}
        if set(result) != set(names) or any(not isinstance(value, str) or not value for value in result.values()):
            raise ValidationRunnerError("validation capability credentials are incomplete")
        return result


class CandidateTrees(Protocol):
    @contextmanager
    def materialize(self, gate: dict[str, Any]) -> Iterator[Path]: ...


class ArtifactCandidateTrees:
    """Materialize an exact-head artifact fetched through the bearer-only client."""
    def __init__(self, client: Any):
        self.client = client

    @contextmanager
    def materialize(self, gate: dict[str, Any]) -> Iterator[Path]:
        candidate = gate.get("candidate")
        subject = gate.get("subject")
        if not isinstance(candidate, dict) or candidate.get("head_sha") != subject:
            raise ValidationRunnerError("candidate artifact is not bound to the validation subject")
        source_digest = candidate.get("source_artifact_sha256")
        patch_digest = candidate.get("patch_artifact_sha256")
        if (not isinstance(source_digest, str) or not _DIGEST.fullmatch(source_digest)
                or not isinstance(patch_digest, str) or not _DIGEST.fullmatch(patch_digest)
                or candidate.get("patch_sha256") != patch_digest):
            raise ValidationRunnerError("candidate artifact reference is invalid")
        source = self.client.get_artifact(source_digest)
        patch = self.client.get_artifact(patch_digest)
        if (not isinstance(source, bytes) or hashlib.sha256(source).hexdigest() != source_digest
                or not isinstance(patch, bytes) or hashlib.sha256(patch).hexdigest() != patch_digest):
            raise ValidationRunnerError("candidate artifact digest changed")
        with tempfile.TemporaryDirectory(prefix="validation-candidate-") as temporary:
            root = Path(temporary) / "tree"
            root.mkdir(mode=0o700)
            try:
                archive = gzip.GzipFile(fileobj=io.BytesIO(source), mode="rb")
                with tarfile.open(fileobj=archive, mode="r|") as tar:
                    count = total = 0
                    for member in tar:
                        count += 1
                        total += max(member.size, 0)
                        target = root / member.name
                        if (count > _MAX_EXTRACTED_FILES or total > _MAX_EXTRACTED_BYTES or not member.name
                                or target.is_symlink() or not target.resolve().is_relative_to(root)
                                or not member.isfile() and not member.isdir()):
                            raise ValidationRunnerError("candidate artifact contains an unsafe entry")
                        tar.extract(member, root, set_attrs=False, filter="data")
                applied = subprocess.run(["/usr/bin/git", "apply", "--whitespace=nowarn", "-"], cwd=root,
                                         input=patch, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                         env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, timeout=30, check=False)
                if applied.returncode:
                    raise ValidationRunnerError("candidate patch could not be applied")
                for path in sorted(root.rglob("*"), reverse=True):
                    if path.is_symlink():
                        raise ValidationRunnerError("candidate artifact contains a symlink")
                    path.chmod(0o500 if path.is_dir() else 0o400)
            except (OSError, EOFError, subprocess.TimeoutExpired, tarfile.TarError) as exc:
                raise ValidationRunnerError("candidate artifact cannot be safely materialized") from exc
            yield root


def _ndjson(output: bytes) -> list[dict[str, Any]]:
    if not output or len(output) > _MAX_OUTPUT:
        raise ValidationRunnerError("validation command produced no bounded NDJSON")
    records = []
    for raw in output.splitlines():
        if not raw or len(raw) > _MAX_NDJSON_LINE:
            raise ValidationRunnerError("validation command NDJSON is outside bounds")
        try:
            record = json.loads(raw)
        except ValueError as exc:
            raise ValidationRunnerError("validation command emitted invalid NDJSON") from exc
        if not isinstance(record, dict):
            raise ValidationRunnerError("validation command NDJSON record is invalid")
        records.append(record)
    if not records or len(records) > 1_000:
        raise ValidationRunnerError("validation command record count is outside bounds")
    return records


def _record(records: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    matches = [record for record in records if record.get("kind") == kind]
    if len(matches) != 1:
        raise ValidationRunnerError(f"validation command must emit exactly one {kind} record")
    return matches[0]


def _require_strings(value: object, expected: tuple[str, ...], label: str) -> None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValidationRunnerError(f"validation {label} record is invalid")
    if not set(expected) <= set(value):
        raise ValidationRunnerError(f"validation {label} assertion failed")




def _assert_recipe(request: RecipeRequest, records: list[dict[str, Any]], command: CommandSpec) -> dict[str, Any]:
    if request.recipe in {"public_source_smoke", "public_source_reconciliation"}:
        reconciliation = None
        if request.recipe == "public_source_reconciliation":
            reconciliation = _record(records, "reconciliation")
            records = [record for record in records if record is not reconciliation]
            if (reconciliation.get("baseline_count") != command.baseline_count
                    or reconciliation.get("source_record_count") != command.baseline_count
                    or reconciliation.get("matched_count") != len(records)
                    or not isinstance(reconciliation.get("response_sha256"), str)
                    or not _DIGEST.fullmatch(reconciliation["response_sha256"])):
                raise ValidationRunnerError("public source reconciliation coverage assertion failed")
        kinds = request.assertions["required_record_types"]
        _require_strings([record.get("type") for record in records if isinstance(record.get("type"), str)], kinds, "extractor")
        identifiers = [record.get("id") for record in records]
        if (not all(isinstance(value, str) and value and len(value) <= 512 for value in identifiers)
                or len(set(identifiers)) != len(identifiers)):
            raise ValidationRunnerError("public source extractor identifiers are invalid")
        evidence = {
            "record_count": len(records), "source_url": request.assertions["source_url"],
            "sample_records": [{"type": record["type"],
                               "id_sha256": hashlib.sha256(record["id"].encode()).hexdigest()}
                              for record in records[:3]],
        }
        if reconciliation is not None:
            evidence["reconciliation"] = reconciliation
        return evidence
    if request.recipe == "warehouse_check":
        record = _record(records, "warehouse")
        if record.get("relation") != request.assertions["expected_relation"]:
            raise ValidationRunnerError("warehouse relation assertion failed")
        _require_strings(record.get("columns"), request.assertions["expected_columns"], "warehouse columns")
        return {"relation": record["relation"], "column_count": len(record["columns"])}
    if request.recipe == "disposable_schema_migration":
        record = _record(records, "migration")
        if record.get("revision") != request.assertions["expected_revision"] or record.get("disposable") is not True:
            raise ValidationRunnerError("disposable schema migration assertion failed")
        return {"revision": record["revision"], "disposable": True}
    if request.recipe == "dag_inspection":
        record = _record(records, "dag")
        if record.get("dag_id") != request.assertions["dag_id"]:
            raise ValidationRunnerError("DAG identifier assertion failed")
        _require_strings(record.get("tasks"), request.assertions["expected_tasks"], "DAG tasks")
        return {"dag_id": record["dag_id"], "task_count": len(record["tasks"])}
    record = _record(records, "lightdash")
    if record.get("project_uuid") != request.assertions["project_uuid"] or record.get("explore") != request.assertions["explore"]:
        raise ValidationRunnerError("Lightdash preview identity assertion failed")
    _require_strings(record.get("fields"), request.assertions["fields"], "Lightdash fields")
    return {"project_uuid": record["project_uuid"], "explore": record["explore"], "field_count": len(record["fields"])}


@cache
def _summary_contract():
    path = Path(__file__).resolve().parents[1] / "orchestration" / "include" / "run_metadata.py"
    spec = importlib.util.spec_from_file_location("validation_run_metadata", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_summary_evidence(stderr: bytes) -> dict[str, Any]:
    contract = _summary_contract()
    try:
        payload, _ = contract.extract_summary_line(stderr.decode("utf-8"))
        if payload is None:
            raise ValidationRunnerError("mandatory source summary is missing")
        summary = contract.validate_summary(json.loads(payload))
    except (ValueError, UnicodeError) as exc:
        raise ValidationRunnerError("mandatory source summary is invalid") from exc
    if summary.get("health") == "failed" or summary.get("completeness") == "failed":
        raise ValidationRunnerError("mandatory summary reports a failed source run")
    partitions = summary.get("partitions", {})
    metrics = summary.get("metrics", {})
    return {
        "validator": "orchestration/include/run_metadata.py",
        "summary_count": 1,
        "byte_count": len((contract.SUMMARY_PREFIX + payload).encode("utf-8")),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "health": summary.get("health"),
        "completeness": summary.get("completeness"),
        "requests": summary.get("requests", {}),
        "partitions": {key: value for key, value in partitions.items() if key != "failures"},
        "partition_failure_samples": len(partitions.get("failures", [])),
        "metrics": {key: value for key, value in metrics.items()
                    if len(key) <= 100 and type(value) in (bool, int, float)},
        "metric_sample_counts": {key: len(value) for key, value in metrics.items()
                                 if len(key) <= 100 and isinstance(value, list)},
    }


class ValidationRunner:
    def __init__(self, *, catalog: CommandCatalog, candidates: CandidateTrees, executor: RestrictedExecutor,
                 credentials: CredentialBroker):
        self.catalog = catalog
        self.candidates = candidates
        self.executor = executor
        self.credentials = credentials

    @property
    def capabilities(self) -> set[str]:
        return self.executor.capabilities

    def execute(self, gate: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        started = time.monotonic()
        started_at = datetime.now(timezone.utc).isoformat()
        request = parse_recipe(recipe=gate.get("recipe"), recipe_args=gate.get("recipe_args"),
                               required_capability=gate.get("required_capability"), subject=gate.get("subject"))
        if request.required_capability not in self.capabilities:
            raise ValidationCapabilityUnavailable("validation capability is unavailable to this runner")
        command = self.catalog.resolve(request)
        environment = self.credentials.environment(request.required_capability, command.credential_env)
        with self.candidates.materialize(gate) as candidate:
            result = self.executor.run(command, candidate, environment)
        output_sha256 = hashlib.sha256(result.output).hexdigest()
        if result.timed_out:
            if request.recipe in {"public_source_smoke", "public_source_reconciliation"}:
                raise ValidationSourceUnavailable("public source validation command timed out")
            raise ValidationRunnerError("validation command timed out")
        if result.exit_code != 0:
            detail = result.stderr.decode("utf-8", errors="replace")[-2000:]
            if command.recipe == "public_source_reconciliation" and "reference source authorization unavailable" in detail:
                raise ValidationCapabilityUnavailable("public reference source authorization unavailable")
            if command.recipe in {"public_source_smoke", "public_source_reconciliation"}:
                raise ValidationSourceCheckFailed(f"public source check exited {result.exit_code}: {detail}")
            raise ValidationRunnerError(f"validation command exited {result.exit_code}: {detail}")
        assertions = _assert_recipe(request, _ndjson(result.output), command)
        if command.summary_required:
            assertions["run_summary"] = _run_summary_evidence(result.stderr)
        duration_ms = int((time.monotonic() - started) * 1000)
        evidence = {
            "label": f"{request.recipe} validation",
            "observation": f"exact subject {request.subject}; command {command.command_id}; assertions passed",
            "subject": request.subject,
            "recipe": request.recipe,
            "command_id": command.command_id,
            "workflow": "bots/validation_runner.py",
            "validation_run_id": uuid.uuid4().hex,
            "started_at": started_at,
            "catalog_argv": [arg if len(arg) <= 256 else
                             {"sha256": hashlib.sha256(arg.encode()).hexdigest()}
                             for arg in command.argv],
            "exit_status": result.exit_code,
            "invocation_sha256": hashlib.sha256(json.dumps(command.argv).encode()).hexdigest(),
            "duration_ms": duration_ms,
            "output_sha256": output_sha256,
            "sha256": hashlib.sha256(json.dumps(assertions, sort_keys=True).encode()).hexdigest(),
            "assertions": assertions,
        }
        return "passed", evidence

    def run_once(self, client: Any, *, runner_id: str, limit: int = 20) -> list[dict[str, Any]]:
        """Claim, start, and durably finish a bounded batch; no client-side retry exists."""
        claimed = client.claim_validation_gates(limit=limit, runner_id=runner_id)
        if not isinstance(claimed, list):
            raise ValidationRunnerError("validation claim response is invalid")
        results = []
        for gate in claimed:
            gate_id = gate.get("gate_id", gate.get("id"))
            if not isinstance(gate_id, str):
                raise ValidationRunnerError("validation claim gate identity is invalid")
            active = client.start_validation_gate(
                gate_id, version=gate["version"], lease_id=gate["lease_id"],
                lease_run_id=gate["lease_run_id"], subject=gate["subject"],
            )
            try:
                status, evidence = self.execute(gate)
            except (ValidationRecipeError, ValidationRunnerError) as exc:
                from airflow._shared.secrets_masker import redact
                from bot_runner import _failure_detail
                diagnostic = str(redact(str(exc), "validation_error", max_depth=20))
                status = "failed"
                category = (
                    ("Validation capability unavailable", "validation_capability_unavailable")
                    if isinstance(exc, ValidationCapabilityUnavailable) else
                    ("Validation source unavailable", "validation_source_unavailable")
                    if isinstance(exc, ValidationSourceUnavailable) else
                    ("Validation source check failed", "validation_source_check_failed")
                    if isinstance(exc, ValidationSourceCheckFailed) else
                    ("Validation runner failure", _failure_detail(RuntimeError(diagnostic))[:200])
                )
                evidence = {
                    "label": category[0],
                    "observation": f"exact subject {gate.get('subject', '')}; {type(exc).__name__}",
                    "subject": gate.get("subject"), "recipe": gate.get("recipe"),
                    "reason_code": category[1],
                    "diagnostic_tail": _failure_detail(RuntimeError(diagnostic[-2000:])),
                    "sha256": hashlib.sha256(str(exc).encode()).hexdigest(),
                }
            finished = client.finish_validation_gate(
                gate_id, version=active["version"], lease_id=gate["lease_id"],
                lease_run_id=gate["lease_run_id"], subject=gate["subject"],
                status=status, evidence=evidence,
            )
            results.append(finished)
        return results


def runner_from_environment(*, catalog: dict[str, Any], credentials: dict[str, dict[str, str]], client: Any) -> ValidationRunner:
    """Build the real worker from deployment-owned code and capabilities."""
    launcher = os.environ.get("BOT_VALIDATION_SANDBOX_LAUNCHER", "")
    from airflow.providers.vintage.bot_dashboard.validation_recipes import available_capabilities
    capabilities = available_capabilities()
    if not capabilities:
        raise ValidationCapabilityUnavailable("validation runner deployment has no validation capability")
    executor: RestrictedExecutor
    if launcher:
        executor = SandboxExecutor(Path(launcher), capabilities)
    else:
        executor = CatalogBwrapExecutor(capabilities)
    return ValidationRunner(catalog=CommandCatalog.parse(catalog), candidates=ArtifactCandidateTrees(client),
                            executor=executor, credentials=StaticCredentialBroker(credentials))


def run(context: dict[str, Any]) -> dict[str, Any]:
    """Airflow task entrypoint for the configured validation lane."""
    from provider_dashboard import DashboardClient
    from validation_catalog import COMMANDS
    from airflow.providers.vintage.bot_dashboard.validation_recipes import available_capabilities
    capabilities = available_capabilities()
    client = DashboardClient.from_environment()
    client.recover_validation_gates()
    workflows = client.poll_validation_workflows(limit=20)
    if not capabilities:
        # Re-check admitted gates against the now-missing installed capability,
        # persisting a blocked result rather than silently idling.
        client.claim_validation_gates(limit=100, runner_id="validation.capability-check")
        return {"status": "capability_unavailable", "items": workflows}
    ti = context["ti"]
    identity = f"{ti.dag_id}\x00{context['dag_run'].run_id}\x00{ti.task_id}".encode()
    runner_id = "validation." + hashlib.sha256(identity).hexdigest()
    worker = runner_from_environment(catalog=COMMANDS, credentials={}, client=client)
    return {"items": workflows + worker.run_once(client, runner_id=runner_id, limit=1)}
