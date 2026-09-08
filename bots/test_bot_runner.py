from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import bot_runner
import providers


def setUpModule() -> None:
    """Never contend with the deployment's shared inference slot directory."""
    global _LOCKS
    _LOCKS = tempfile.TemporaryDirectory(prefix="bot-test-locks-")
    bot_runner.LOCKS_DIR = pathlib.Path(_LOCKS.name)


def tearDownModule() -> None:
    _LOCKS.cleanup()


IDENTITY = {
    "dag_id": "bot__demo",
    "run_id": "run-1",
    "task_id": "run",
    "map_index": -1,
    "try_number": 1,
}

def _cfg(tmp: str, **overrides) -> dict:
    root = pathlib.Path(tmp)
    bot = root / "demo"
    bot.mkdir(exist_ok=True)
    (bot / "prompt.md").write_text("prompt {{CTX}}")
    value = {
        "name": "source_discovery",
        "dir": str(bot),
        "schedule": "manual",
        "prompt": "prompt.md",
        "timeout_minutes": 1,
        "context_budget_minutes": 1,
        "model_budget_minutes": 1,
        "cleanup_margin_seconds": 1,
        "capacity_policy": "skip",
        "retry_on": [],
        "context": {},
        "output": {"format": "json", "schema": "source_discovery_v2"},
    }
    value.update(overrides)
    return value


def _models(tmp: str, aliases=("fake",), *, capabilities=None) -> pathlib.Path:
    models = {
        "default": aliases[0],
        "concurrency": {"max_active": 2},
        "models": {
            alias: {
                "provider": "command",
                "model": "fake",
                "argv": ["/bin/echo", "{}"],
                "capabilities": capabilities or [],
            }
            for alias in aliases
        },
    }
    path = pathlib.Path(tmp) / "models.yml"
    path.write_text(json.dumps(models))
    return path


class ReportContractTest(unittest.TestCase):
    def test_prompt_includes_actual_report_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp, _context_values={"CTX": {}})
            budget = bot_runner.RunBudget(dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1), 0)
            prompt, _, _ = bot_runner.build_prompt(cfg, budget)
            schema = json.loads(prompt.split("exact output schema:\n", 1)[1])
            self.assertEqual("SourceDiscoveryV2", schema["title"])
            self.assertIn("proposals", schema["properties"])

    def test_schema_failure_reports_fields_without_model_values(self):
        with self.assertRaises(bot_runner.BotError) as caught:
            bot_runner._json_report('{"private_value":"sensitive-example"}', {"output": {"schema": "source_discovery_v2"}})
        self.assertIn("missing", str(caught.exception))
        self.assertNotIn("sensitive-example", str(caught.exception))


class RunnerDeadlineTest(unittest.TestCase):
    def test_one_wall_clock_budget_exhaustion_skips_context_model_and_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp)
            class Client:
                deadline_at = None
                def claim_budget(self, identity, total):
                    return {"deadline_at": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()}
                def submit_run(self, envelope):
                    self.envelope = envelope
                    return {"outcome": envelope["outcome"], "retry_class": envelope["retry_class"], "execution": envelope["identity"]}

            client = Client()
            with mock.patch.object(bot_runner, "build_prompt", side_effect=AssertionError("context invoked")), \
                 mock.patch.object(bot_runner.providers, "get_provider", side_effect=AssertionError("model invoked")):
                result = bot_runner.run(cfg, identity=IDENTITY, models_config=_models(tmp), client=client)
            self.assertEqual((result.outcome, result.retry_class, result.reason_code), ("timed_out", "terminal", "deadline_exhausted"))
            self.assertEqual(client.envelope["attempts"], [])
            self.assertIsNone(client.envelope["payload"])

    def test_command_and_http_deadlines_are_timeouts_not_capacity(self):
        model = {"provider": "command", "argv": [sys.executable, "-c", "import time; time.sleep(2)"], "timeout_s": 0.05}
        with self.assertRaises(providers.ProviderTimeout) as caught:
            providers.command(model, "prompt")
        self.assertNotIsInstance(caught.exception, providers.ProviderBusy)

        class TimedOut:
            def __enter__(self):
                raise AssertionError("context manager not expected")
            def __exit__(self, *args):
                return False

        with mock.patch.object(providers.urllib.request, "urlopen", side_effect=TimeoutError("socket timed out")):
            with self.assertRaises(providers.ProviderTimeout) as caught:
                providers.openai_chat({"endpoint": "http://provider.invalid", "model": "m"}, "prompt", timeout_s=.05)
        self.assertNotIsInstance(caught.exception, providers.ProviderBusy)

    def test_terminal_provider_failure_does_not_try_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp)
            models = _models(tmp, ("primary", "fallback"))
            calls = []
            def invoke(model, prompt, **kwargs):
                calls.append(model["alias"])
                raise providers.ProviderTerminal("bad schema")
            with mock.patch.object(bot_runner.providers, "get_provider", return_value=invoke):
                result = bot_runner.run(cfg, identity=IDENTITY, models_config=models, ephemeral=True, context_values={"CTX": {}})
            self.assertEqual(calls, ["primary"])
            self.assertEqual((result.outcome, result.retry_class), ("failed", "terminal"))

    def test_missing_credential_is_terminal_without_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp, model=["primary", "fallback"])
            path = _models(tmp, ("primary", "fallback"))
            value = json.loads(path.read_text())
            value["models"]["primary"].update({
                "provider": "openai_chat",
                "endpoint": "http://provider.invalid",
                "model": "m",
                "api_key_env": "MISSING_TEST_CREDENTIAL",
            })
            path.write_text(json.dumps(value))
            with mock.patch.dict(os.environ, {}, clear=False), mock.patch.object(bot_runner.providers, "get_provider", side_effect=AssertionError("model invoked")):
                result = bot_runner.run(cfg, identity=IDENTITY, models_config=path, ephemeral=True, context_values={"CTX": {}})
            self.assertEqual((result.outcome, result.retry_class), ("failed", "terminal"))

    def test_capacity_obeys_skip_or_retry_policy(self):
        for policy, retry_on, expected in (("skip", [], "none"), ("retry", ["capacity"], "capacity")):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as tmp:
                cfg = _cfg(tmp, capacity_policy=policy, retry_on=retry_on)
                def invoke(model, prompt, **kwargs):
                    raise providers.ProviderBusy("at capacity")
                with mock.patch.object(bot_runner.providers, "get_provider", return_value=invoke):
                    result = bot_runner.run(cfg, identity=IDENTITY, models_config=_models(tmp), ephemeral=True, context_values={"CTX": {}})
                self.assertEqual(result.outcome, "capacity_unavailable")
                self.assertEqual(result.retry_class, expected)

    def test_typed_json_gate_skips_without_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp, context={"CTX": {"command": [sys.executable, "-c", "import json; print(json.dumps({'pending': 0}))"], "timeout_s": 2}}, gate={"context": "CTX", "path": "pending", "operator": "equals", "value": 0, "reason_code": "gate_no_work"})
            with mock.patch.object(bot_runner.providers, "get_provider", side_effect=AssertionError("model invoked")):
                result = bot_runner.run(cfg, identity=IDENTITY, models_config=_models(tmp), ephemeral=True)
            self.assertEqual((result.outcome, result.retry_class, result.reason_code), ("skipped", "none", "gate_no_work"))

    def test_xcom_is_body_free(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp, gate={"context": "CTX", "path": "healthy", "operator": "equals", "value": True, "reason_code": "gate_no_work"})
            result = bot_runner.run(cfg, identity=IDENTITY, context_values={"CTX": {"healthy": True, "secret": "credential"}}, ephemeral=True)
            projection = result.xcom()
            encoded = json.dumps(projection)
            self.assertEqual(set(projection), {"outcome", "retry_class", "reason_code", "execution"})
            for forbidden in ("payload", "context", "credential", "secret", str(pathlib.Path(tmp))):
                self.assertNotIn(forbidden, encoded)


class InferenceSlotTest(unittest.TestCase):
    def test_partial_acquisition_releases_already_acquired_slot(self):
        first = mock.Mock()
        with mock.patch.object(bot_runner, "_acquire_one", side_effect=[first, None]), \
             mock.patch.object(bot_runner.fcntl, "flock") as flock:
            with self.assertRaises(providers.ProviderBusy):
                with bot_runner._inference_slot({"concurrency": {"max_active": 1}}, {"alias": "m", "max_concurrency": 1}):
                    pass
        self.assertEqual(first.close.call_count, 1)
        flock.assert_called_once_with(first, bot_runner.fcntl.LOCK_UN)


class CommandChildStdinTest(unittest.TestCase):
    """An argv prompt must leave the child no inherited stdin to block on."""

    SCRIPT = "import sys; print('eof' if sys.stdin.read() == '' else 'data')"

    def _run(self, argv: list[str], prompt: str) -> str:
        reader, _writer = os.pipe()
        saved = os.dup(0)
        os.dup2(reader, 0)  # emulate a worker whose stdin pipe never closes
        try:
            return providers.command(
                {"provider": "command", "argv": argv, "inherit_env": False, "pass_env": [], "timeout_s": 30},
                prompt,
            ).text.strip()
        finally:
            os.dup2(saved, 0)
            os.close(saved)
            os.close(reader)
            os.close(_writer)

    def test_argv_prompt_closes_child_stdin(self):
        argv = [sys.executable, "-c", self.SCRIPT, "{prompt}"]
        self.assertEqual("eof", self._run(argv, "argv-prompt"))

    def test_stdin_prompt_still_reaches_the_child(self):
        argv = [sys.executable, "-c", "import sys; sys.stdout.write(sys.stdin.read())"]
        self.assertEqual("piped-prompt", self._run(argv, "piped-prompt"))


class StrictDefinitionValidationTest(unittest.TestCase):
    def test_all_tracked_bots_and_models_validate_strictly(self):
        for path in bot_runner.discover():
            bot_runner.load_bot(path.parent)
        bot_runner.load_models("bots/models.yml")
        bot_runner.load_models("bots/models.example.yml")

    def test_unknown_keys_output_schema_trigger_and_fallback_capability_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            valid = _cfg(tmp)
            for name, mutate in (
                ("unknown", lambda c: c.update(mystery=True)),
                ("output", lambda c: c.update(output={"format": "text", "schema": "x"})),
                ("schema", lambda c: c.update(output={"format": "json"})),
            ):
                bot = root / name
                bot.mkdir()
                (bot / "prompt.md").write_text("{{CTX}}")
                candidate = dict(valid, name=name, dir=str(bot))
                mutate(candidate)
                (bot / "bot.yml").write_text(json.dumps(candidate))
                with self.assertRaises(bot_runner.BotError):
                    bot_runner.load_bot(bot)

            bot = root / "triggered"
            bot.mkdir()
            (bot / "prompt.md").write_text("{{CTX}}")
            candidate = dict(valid, name="triggered", dir=str(bot), triggers=["missing"])
            (bot / "bot.yml").write_text(json.dumps(candidate))
            with self.assertRaises(bot_runner.BotError):
                bot_runner.load_bot(bot)

            # Every fallback must retain the bot's required capability.
            cfg = dict(valid, requires_capabilities=["web"], model=["primary", "fallback"])
            models = {
                "default": "primary",
                "models": {
                    "primary": {"provider": "command", "argv": ["/bin/echo"], "capabilities": ["web"]},
                    "fallback": {"provider": "command", "argv": ["/bin/echo"], "capabilities": []},
                },
                "bots": {"source_discovery": ["primary", "fallback"]},
            }
            model_path = root / "models.json"
            model_path.write_text(json.dumps(models))
            with self.assertRaises(bot_runner.BotError):
                bot_runner.resolve_bot_models(cfg, bot_runner.load_models(model_path))

    def test_budget_rule_rejects_context_plus_model_plus_cleanup_over_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp, timeout_minutes=1, context_budget_minutes=1, model_budget_minutes=1, cleanup_margin_seconds=1, context={"CTX": {"command": ["echo"], "timeout_s": 61}})
            bot = pathlib.Path(tmp) / "demo"
            (bot / "bot.yml").write_text(json.dumps(cfg))
            with self.assertRaises(bot_runner.BotError):
                bot_runner.load_bot(bot)


if __name__ == "__main__":
    unittest.main()
