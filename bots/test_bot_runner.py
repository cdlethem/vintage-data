"""Contract tests for the bot layer: model selection, gating, and failure modes.

Run with an interpreter that has PyYAML:
    orchestration/.venv/bin/python -m unittest discover -s bots -t bots
"""
import json
import pathlib
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import bot_runner  # noqa: E402
import providers  # noqa: E402

MODELS = textwrap.dedent("""
    default: fake
    models:
      fake:
        provider: command
        model: whatever
        argv: ["/bin/cat"]
      other:
        provider: command
        model: whatever
        argv: ["/bin/echo", "from-other"]
      keyed:
        provider: openai_chat
        endpoint: http://127.0.0.1:1/v1/chat/completions
        model: m
        api_key_env: TEST_KEY_THAT_IS_UNSET
""")


class BotFixture:
    """A bot whose only context command is `echo`, so tests need no network."""

    def __init__(self, tmp: str, *, gate=True, model=None, context_status="STATUS: OK"):
        self.root = pathlib.Path(tmp)
        (self.root / "demo").mkdir()
        (self.root / "demo" / "prompt.md").write_text("digest follows:\n{{DIGEST}}\n")
        cfg = {
            "name": "demo",
            "schedule": "manual",
            "prompt": "prompt.md",
            "context": {"DIGEST": {"command": ["/bin/echo", context_status]}},
        }
        if model:
            cfg["model"] = model
        if gate:
            cfg["gate"] = {"context": "DIGEST", "skip_if_matches": "(?m)^STATUS: OK",
                           "skip_report": "all clear"}
        (self.root / "demo" / "bot.yml").write_text(json.dumps(cfg))
        self.models = self.root / "models.yml"
        self.models.write_text(MODELS)

    def load(self):
        return bot_runner.load_bot(self.root / "demo")


class ModelSelectionTest(unittest.TestCase):
    def test_alias_defaults_to_the_machine_default(self):
        cfg = bot_runner.load_models_from_text = None  # guard against typos below
        del cfg
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            models = bot_runner.load_models(fixture.models)
            self.assertEqual(bot_runner.resolve_model(None, models)["alias"], "fake")

    def test_unknown_alias_names_the_available_ones(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            models = bot_runner.load_models(fixture.models)
            with self.assertRaises(bot_runner.BotError) as caught:
                bot_runner.resolve_model("gpt-9", models)
            message = str(caught.exception)
            self.assertIn("gpt-9", message)
            self.assertIn("fake", message)      # never silently substitutes

    def test_bot_alias_wins_over_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp, gate=False, model="other")
            with mock.patch.object(bot_runner, "RUNS_DIR", pathlib.Path(tmp) / "runs"):
                result = bot_runner.run(fixture.load(), models_config=fixture.models)
            self.assertEqual(result.model_alias, "other")
            self.assertIn("from-other", result.report)

    def test_missing_api_key_names_the_env_var(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            models = bot_runner.load_models(fixture.models)
            model = bot_runner.resolve_model("keyed", models)
            with self.assertRaises(providers.ProviderError) as caught:
                providers.openai_chat(model, "hi")
            self.assertIn("TEST_KEY_THAT_IS_UNSET", str(caught.exception))


class GateTest(unittest.TestCase):
    def test_gate_skips_the_model_entirely(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            # No models.yml passed at all: a gated run must not need model wiring.
            with mock.patch.object(bot_runner, "RUNS_DIR", pathlib.Path(tmp) / "runs"), \
                 mock.patch.object(providers, "get_provider",
                                   side_effect=AssertionError("model was called")):
                result = bot_runner.run(fixture.load(), models_config=tmp + "/missing.yml")
            self.assertEqual(result.status, bot_runner.SKIPPED)
            self.assertIsNone(result.model_alias)
            self.assertIn("all clear", result.report)
            self.assertTrue(pathlib.Path(result.report_path).is_file())

    def test_gate_miss_reaches_the_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp, context_status="STATUS: PROBLEM")
            with mock.patch.object(bot_runner, "RUNS_DIR", pathlib.Path(tmp) / "runs"):
                result = bot_runner.run(fixture.load(), models_config=fixture.models)
            self.assertEqual(result.status, "ok")
            self.assertEqual(result.model_alias, "fake")
            # /bin/cat echoes the prompt back, proving context substitution ran.
            self.assertIn("STATUS: PROBLEM", result.report)


class PromptTest(unittest.TestCase):
    def test_unfilled_placeholder_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            (fixture.root / "demo" / "prompt.md").write_text("{{DIGEST}} and {{MISSING}}")
            with self.assertRaises(bot_runner.BotError) as caught:
                bot_runner.build_prompt(fixture.load())
            self.assertIn("MISSING", str(caught.exception))

    def test_failed_context_command_fails_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            cfg = fixture.load()
            cfg["context"] = {"DIGEST": {"command": ["/bin/false"]}}
            with self.assertRaises(bot_runner.BotError):
                bot_runner.build_prompt(cfg)

    def test_repo_relative_program_resolves_from_any_cwd(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            cfg = fixture.load()
            # argv[0] is repo-relative and cwd is a different directory, so a
            # cwd-relative reading of the program path would not exist at all.
            # free_slots is tracked, stdlib-only, and exits 0 against a dead
            # endpoint ("unknown" is not "busy").
            cfg["context"] = {"DIGEST": {"command": ["bots/bin/free_slots",
                                                     "http://127.0.0.1:1/slots"],
                                         "cwd": "monitoring"}}
            prompt, context = bot_runner.build_prompt(cfg)
            self.assertEqual(context["DIGEST"], "")
            self.assertIn("digest follows:", prompt)

    def test_unresolvable_program_path_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = BotFixture(tmp)
            cfg = fixture.load()
            cfg["context"] = {"DIGEST": {"command": ["bots/bin/nope", "x"]}}
            with self.assertRaises(bot_runner.BotError) as caught:
                bot_runner.build_prompt(cfg)
            self.assertIn("bots/bin/nope", str(caught.exception))


class ProviderTest(unittest.TestCase):
    def test_command_provider_exit_3_means_busy(self):
        model = {"alias": "busy", "provider": "command", "model": "m",
                 "argv": ["/bin/sh", "-c", "exit 3"]}
        with self.assertRaises(providers.ProviderBusy):
            providers.command(model, "prompt")

    def test_command_provider_reports_a_missing_binary(self):
        model = {"alias": "gone", "provider": "command", "model": "m",
                 "argv": ["/nonexistent/model-cli"]}
        with self.assertRaises(providers.ProviderError) as caught:
            providers.command(model, "prompt")
        self.assertIn("model-cli", str(caught.exception))

    def test_empty_completion_explains_a_truncated_reasoning_model(self):
        response = {"choices": [{"finish_reason": "length",
                                 "message": {"content": "",
                                             "reasoning_content": "thinking " * 50}}]}
        model = {"alias": "local", "provider": "openai_chat", "model": "m",
                 "endpoint": "http://example.invalid/v1/chat/completions"}
        with mock.patch.object(providers, "_post_json", return_value=response):
            with self.assertRaises(providers.ProviderError) as caught:
                providers.openai_chat(model, "prompt")
        message = str(caught.exception)
        self.assertIn("finish_reason=length", message)
        self.assertIn("max_tokens", message)

    def test_unknown_provider_is_rejected(self):
        with self.assertRaises(providers.ProviderError):
            providers.get_provider("telepathy")


class DiscoveryTest(unittest.TestCase):
    def test_repo_bots_load_and_name_reachable_prompts(self):
        paths = bot_runner.discover()
        self.assertTrue(paths, "no bot definitions found")
        for path in paths:
            cfg = bot_runner.load_bot(path)
            with self.subTest(bot=cfg["name"]):
                self.assertTrue((pathlib.Path(cfg["dir"]) / cfg["prompt"]).is_file())
                self.assertTrue(cfg["schedule"])


if __name__ == "__main__":
    unittest.main()
