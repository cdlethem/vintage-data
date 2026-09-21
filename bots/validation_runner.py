"""Run catalogued validation recipes without executing candidate-selected commands.

The control plane leases a gate before this runner sees it.  Every executable is
selected by a root-installed operator catalog; gate arguments are assertions,
not command input.  Candidate source is an immutable, exact-head artifact
unpacked into a private read-only tree.
"""
from __future__ import annotations

import gzip
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
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Protocol

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

    @classmethod
    def parse(cls, command_id: str, value: object) -> "CommandSpec":
        if not _TOKEN.fullmatch(command_id) or not isinstance(value, dict):
            raise ValidationRunnerError("validation command catalog is invalid")
        expected = {"recipe", "capability", "argv", "timeout_seconds", "credential_env", "network_profile", "source_url", "expected_status"}
        if set(value) - expected or {"recipe", "capability", "argv", "timeout_seconds"} - set(value):
            raise ValidationRunnerError("validation command catalog has unknown fields")
        recipe, capability, argv, timeout = value["recipe"], value["capability"], value["argv"], value["timeout_seconds"]
        if not isinstance(recipe, str) or not isinstance(capability, str) or not _TOKEN.fullmatch(recipe) or not _TOKEN.fullmatch(capability):
            raise ValidationRunnerError("validation command catalog has invalid identifiers")
        if (not isinstance(argv, list) or not 1 <= len(argv) <= 32 or any(
                not isinstance(part, str) or not part or len(part) > 1024 or "\x00" in part for part in argv)):
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
        if recipe == "public_source_smoke":
            if not isinstance(source_url, str) or not source_url.startswith("https://") or type(expected_status) is not int:
                raise ValidationRunnerError("public validation catalog assertions are invalid")
        elif source_url is not None or expected_status is not None:
            raise ValidationRunnerError("non-public validation command has public assertions")
        return cls(command_id, recipe, capability, tuple(argv), timeout, tuple(names), profile, source_url, expected_status)


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
        if request.recipe == "public_source_smoke" and (
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
            raise ValidationRunnerError("validation sandbox launcher is unavailable") from exc
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
    """Execute a catalogued, no-network bwrap command with bounded captured output.

    The catalog is trusted application code, not a file from the candidate
    tree.  This executor deliberately rejects live-egress entries: a normal
    network namespace cannot safely select public HTTPS destinations.
    """
    def __init__(self, capabilities: set[str]):
        self._capabilities = frozenset(capabilities)

    @property
    def capabilities(self) -> set[str]:
        return set(self._capabilities)

    def run(self, command: CommandSpec, candidate_root: Path, environment: dict[str, str]) -> CommandResult:
        try:
            info = Path(command.argv[0]).lstat()
        except OSError as exc:
            raise ValidationRunnerError("validation sandbox executable is unavailable") from exc
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
            raise ValidationRunnerError("restricted public egress proxy is unavailable") from exc
        try:
            with public_egress(candidate_root, command.timeout_seconds) as relay:
                socket = relay.socket_path.lstat()
                relay_file = relay.relay_path.lstat()
                if (not stat.S_ISSOCK(socket.st_mode) or socket.st_uid != os.getuid() or socket.st_mode & 0o077
                        or not stat.S_ISREG(relay_file.st_mode) or relay_file.st_uid != 0 or relay_file.st_mode & 0o022):
                    raise ValidationRunnerError("public egress relay is unsafe")
                split = argv.index("--")
                argv = (*argv[:split], "--ro-bind", str(relay.socket_path), "/public-egress.sock",
                        "--ro-bind", str(relay.relay_path), "/opt/validation-relay.py",
                        "--setenv", "HTTP_PROXY", relay.proxy_url, "--setenv", "HTTPS_PROXY", relay.proxy_url,
                        "--setenv", "NO_PROXY", "", "--", *relay.argv_prefix, *argv[split + 1:])
                return self._run(argv, candidate_root, environment, command.timeout_seconds)
        except (OSError, PublicEgressError) as exc:
            raise ValidationRunnerError("restricted public egress proxy is unavailable") from exc

    @staticmethod
    def _run(argv: tuple[str, ...], candidate_root: Path, environment: dict[str, str], timeout_seconds: int) -> CommandResult:
        started = time.monotonic()
        try:
            process = subprocess.Popen(argv, cwd=candidate_root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, start_new_session=True,
                                       env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1", **environment})
        except OSError as exc:
            raise ValidationRunnerError("validation sandbox could not start") from exc
        output = bytearray()
        timed_out = False
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            while selector.get_map():
                remaining = timeout_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    timed_out = True
                    break
                for key, _ in selector.select(min(remaining, 0.25)):
                    chunk = os.read(key.fileobj.fileno(), min(65_536, _MAX_OUTPUT + 1 - len(output)))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    output.extend(chunk)
                    if len(output) > _MAX_OUTPUT:
                        raise ValidationRunnerError("validation command output exceeds its bound")
            if not timed_out:
                try:
                    process.wait(timeout=max(0.001, timeout_seconds - (time.monotonic() - started)))
                except subprocess.TimeoutExpired:
                    timed_out = True
            return CommandResult(-1 if timed_out else process.returncode, bytes(output), timed_out,
                                 int((time.monotonic() - started) * 1000))
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            selector.close()
            process.stdout.close()


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




def _assert_recipe(request: RecipeRequest, records: list[dict[str, Any]]) -> dict[str, Any]:
    if request.recipe == "public_source_smoke":
        kinds = request.assertions["required_record_types"]
        _require_strings([record.get("type") for record in records if isinstance(record.get("type"), str)], kinds, "extractor")
        identifiers = [record.get("id") for record in records]
        if (not all(isinstance(value, str) and value and len(value) <= 512 for value in identifiers)
                or len(set(identifiers)) != len(identifiers)):
            raise ValidationRunnerError("public source extractor identifiers are invalid")
        return {"record_count": len(records), "source_url": request.assertions["source_url"],
                "expected_status": request.assertions["expected_status"]}
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
        request = parse_recipe(recipe=gate.get("recipe"), recipe_args=gate.get("recipe_args"),
                               required_capability=gate.get("required_capability"), subject=gate.get("subject"))
        if request.required_capability not in self.capabilities:
            raise ValidationRunnerError("validation capability is unavailable to this runner")
        command = self.catalog.resolve(request)
        environment = self.credentials.environment(request.required_capability, command.credential_env)
        with self.candidates.materialize(gate) as candidate:
            result = self.executor.run(command, candidate, environment)
        output_sha256 = hashlib.sha256(result.output).hexdigest()
        if result.timed_out:
            raise ValidationRunnerError("validation command timed out")
        if result.exit_code != 0:
            raise ValidationRunnerError(f"validation command exited {result.exit_code}")
        assertions = _assert_recipe(request, _ndjson(result.output))
        duration_ms = int((time.monotonic() - started) * 1000)
        evidence = {
            "label": f"{request.recipe} validation",
            "observation": f"exact subject {request.subject}; command {command.command_id}; assertions passed",
            "subject": request.subject,
            "recipe": request.recipe,
            "command_id": command.command_id,
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
                status = "failed"
                evidence = {
                    "label": "Validation runner failure",
                    "observation": f"exact subject {gate.get('subject', '')}; {type(exc).__name__}",
                    "subject": gate.get("subject"), "recipe": gate.get("recipe"),
                    "reason_code": str(exc)[:200],
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
    capability_text = os.environ.get("BOT_VALIDATION_CAPABILITIES", "")
    capabilities = {item for item in capability_text.split(",") if _TOKEN.fullmatch(item)}
    if not capabilities:
        raise ValidationRunnerError("validation runner deployment is incomplete")
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

    client = DashboardClient.from_environment()
    ti = context["ti"]
    identity = f"{ti.dag_id}\x00{context['dag_run'].run_id}\x00{ti.task_id}".encode()
    runner_id = "validation." + hashlib.sha256(identity).hexdigest()
    worker = runner_from_environment(catalog=COMMANDS, credentials={}, client=client)
    return {"items": worker.run_once(client, runner_id=runner_id, limit=1)}
