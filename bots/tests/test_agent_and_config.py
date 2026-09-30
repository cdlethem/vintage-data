import json
import sys

import pytest

from airflow_bots import agent, config, evals
from airflow_bots.config import Agent


DECISION = ('{"question": "Accept a shorter last page?", "why": "It loosens a guard.", "routine": false, "options": ['
            '{"choice": "Accept it", "pros": "data flows", "cons": "looser", "chosen": true},'
            '{"choice": "Keep the check", "pros": "strict", "cons": "stays broken"}]}')


def test_result_is_the_last_json_object_even_after_prose_and_examples():
    text = """I looked at it. Example: {"action": "none"} was not right.
```json
{"action": "fix", "title": "Feed parser breaks on empty pages", "summary": "It crashed.",
 "details": "Changed {x}", "decision": %s, "duplicate_of": "12"}
```""" % DECISION
    result = agent.parse_result(text)
    assert (result.action, result.duplicate_of, result.details) == ("fix", 12, "Changed {x}")
    assert result.decision.chosen.choice == "Accept it" and result.decision.routine is False


def with_decision(**changes):
    decision = json.loads(DECISION)
    decision.update(changes)
    return json.dumps({"action": "fix", "title": "t", "summary": "s", "decision": decision})


@pytest.mark.parametrize("text", [
    "no json here",
    '{"action": "maybe", "title": "t", "summary": "s"}',
    '{"action": "fix", "title": "", "summary": "s"}',
    '{"action": "fix", "title": "t", "summary": "s"}',  # a fix must lay out its decision
    '{"action": "ask", "title": "t", "summary": "s", "decision": null}',
    with_decision(question=""),
    with_decision(options=[{"choice": "Only one", "pros": "p", "cons": "c", "chosen": True}]),
    with_decision(options=[{"choice": "A", "pros": "p", "cons": "c"}, {"choice": "B", "pros": "p", "cons": "c"}]),
    with_decision(options=[{"choice": "A", "pros": "p", "cons": "c", "chosen": True},
                           {"choice": "B", "pros": "p", "cons": "c", "chosen": True}]),
    with_decision(options=[{"choice": "A", "pros": "p", "cons": "c", "chosen": True}, {"choice": "B", "pros": "p"}]),
])
def test_unusable_results_are_rejected(text):
    with pytest.raises(ValueError):
        agent.parse_result(text)


def test_render_fills_once_and_rejects_missing_values():
    assert agent.render("log: {{log}}", log="{{not_a_placeholder}}") == "log: {{not_a_placeholder}}"
    with pytest.raises(ValueError):
        agent.render("{{missing}}")


def test_agent_run_passes_prompt_on_stdin_and_times_out(tmp_path):
    echo = Agent(command=(sys.executable, "-c", "import sys; print(sys.stdin.read())"), timeout_minutes=1)
    run = agent.run(echo, '{"action": "none", "title": "t", "summary": "s"}', tmp_path, tmp_path / "run")
    assert run.ok and run.result.action == "none"
    slow = Agent(command=(sys.executable, "-c", "import time; time.sleep(5)"), timeout_minutes=0)
    assert "timed out" in agent.run(slow, "x", tmp_path, tmp_path / "slow").error


def test_config_rejects_unknown_keys_and_unset_variables(tmp_path, monkeypatch):
    path = tmp_path / "bots.yml"
    base = "github: {repo: a/b}\nairflow: {url: http://x}\nagents: {default: {command: [x]}}\n"
    path.write_text(base + "state_dir: /tmp/s\nhael: {}\n")
    with pytest.raises(config.ConfigError, match="hael"):
        config.load(path)
    monkeypatch.delenv("NOPE", raising=False)
    path.write_text(base + "state_dir: ${NOPE}/s\n")
    with pytest.raises(config.ConfigError, match="NOPE"):
        config.load(path)


def test_eval_expectations():
    result = agent.Result(action="fix", title="t", summary="s")
    checks = evals.check({"action": ["fix", "ask"], "paths": ["extract/*"], "forbid": ["extract/tests/*"]},
                         result, ["extract/fetch.py", "extract/tests/test_fetch.py"])
    assert checks["action"][0] and checks["paths"][0] and not checks["forbid"][0]
