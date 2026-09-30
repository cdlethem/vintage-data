"""The settings page edits the live config file; a bad edit must never reach the scheduler."""
import pytest
import yaml

pytest.importorskip("airflow")
from airflow_bots import web  # noqa: E402

CONFIG = """
github: {repo: a/b}
airflow: {url: http://x}
state_dir: STATE
agents:
  default: {command: [x], timeout_minutes: 45}
heal:
  lookback_hours: 12
  prompt: heal.md
  auto_merge: {enabled: true, paths: ["src/*"], require_checks: false}
"""


@pytest.fixture
def path(tmp_path):
    (tmp_path / "heal.md").write_text("prompt")
    path = tmp_path / "bots.yml"
    path.write_text(CONFIG.replace("STATE", str(tmp_path / "state")))
    return path


def test_blank_fields_restore_defaults_and_hidden_settings_survive(path):
    raw = yaml.safe_load(path.read_text())
    web.apply_form(raw, "heal", {"enabled": "on", "lookback_hours": "", "auto_merge_paths": "src/*\ntests/*"})
    assert web.write_config(path, raw, "tester") is None
    heal = yaml.safe_load(path.read_text())["heal"]
    assert "lookback_hours" not in heal and heal["prompt"] == "heal.md"
    assert heal["auto_merge"] == {"enabled": False, "paths": ["src/*", "tests/*"], "require_checks": False}
    history = (path.parent / "state" / "config_history.jsonl").read_text()
    assert '"who": "tester"' in history and "lookback_hours" in history


def test_an_edit_that_breaks_the_config_is_not_written(path):
    before = path.read_text()
    raw = yaml.safe_load(before)
    web.apply_form(raw, "agent", {"name": "default", "delete": "yes"})
    assert "define at least one agent" in web.write_config(path, raw, "tester")
    assert path.read_text() == before and not list(path.parent.glob(".*candidate"))


@pytest.mark.parametrize("section, form, message", [
    ("job", {"name": "a b", "schedule": "0 6 * * *"}, "names may only use"),
    ("agent", {"name": "default", "new": "1", "command": "y"}, "already exists"),
    ("limits", {"daily_usd": "five"}, "not a number"),
])
def test_bad_form_input_is_explained(path, section, form, message):
    with pytest.raises(ValueError, match=message):
        web.apply_form(yaml.safe_load(path.read_text()), section, form)


def test_invalid_cron_is_refused_before_airflow_sees_it():
    with pytest.raises(ValueError, match="not a valid cron"):
        web.validate_schedules({"jobs": {"weekly": {"schedule": "61 * * * *"}}})
    web.validate_schedules({"heal": {"schedule": "*/15 * * * *"}})
