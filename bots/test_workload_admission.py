"""Verify intake pressure stops model work without stopping repair bots."""
import datetime as dt
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import bot_runner
from provider_dashboard import ControlPlaneError, DashboardClient
from test_bot_runner import _cfg, _models, IDENTITY
from airflow.providers.vintage.bot_dashboard.report_schemas import RunEnvelopeV1


class WorkloadAdmissionTest(unittest.TestCase):
    def run_bot(self, name, *, allowed=False, error=None, identity=None, **kwargs):
        control = mock.Mock()
        control.claim_budget.return_value = {
            "deadline_at": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1)).isoformat()
        }
        control.workload.return_value = {"allowed": allowed}
        control.workload.side_effect = error
        control.submit_run.side_effect = lambda e: {"execution": e["identity"]}
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(bot_runner, "build_prompt", side_effect=bot_runner.BotError("context reached")) as context, \
             mock.patch.object(bot_runner, "dashboard_model", side_effect=AssertionError("model setup invoked")):
            result = bot_runner.run(_cfg(tmp, name=name, retry_on=["transient"]),
                                    identity=identity or IDENTITY, client=control, **kwargs)
        if control.submit_run.called:
            RunEnvelopeV1.model_validate(control.submit_run.call_args.args[0])
        return result, control, context

    def test_held_discretionary_specialists_save_skip_without_context_or_model(self):
        for name in ("source_discovery", "source_vetting", "source_scheduling",
                     "analytics_engineer", "data_analyst", "cadence_review"):
            with self.subTest(name=name):
                result, control, context = self.run_bot(name)
                self.assertEqual("resolution_capacity_reserved", result.reason_code)
                self.assertEqual("skipped", result.outcome)
                context.assert_not_called()
                envelope = control.submit_run.call_args.args[0]
                self.assertEqual([], envelope["attempts"])
                self.assertIsNone(envelope["payload"])

    def test_allowed_intake_and_operational_bots_reach_context(self):
        gated = {"source_discovery", "source_vetting", "source_scheduling",
                 "analytics_engineer", "data_analyst", "cadence_review"}
        for name in (*sorted(gated), "failure_triage", "manager"):
            with self.subTest(name=name):
                _, control, context = self.run_bot(name, allowed=True)
                context.assert_called_once()
                self.assertEqual(name in gated, control.workload.called)

    def test_existing_ticket_follow_up_keeps_completion_lane(self):
        identity = {**IDENTITY, "run_id": "planning__task__1__r1__source_scheduling"}
        _, control, context = self.run_bot("source_scheduling", identity=identity)
        control.workload.assert_not_called()
        context.assert_called_once()

    def test_requested_parent_reaches_model_even_when_global_backlog_is_empty(self):
        control = mock.Mock()
        control.claim_budget.return_value = {
            "deadline_at": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1)).isoformat()
        }
        control.follow_up_context.return_value = {
            "backlog_count": 1, "selected": {"parent_task_id": "github-parent", "title": "Repair GitHub metadata"},
        }
        control.submit_run.side_effect = lambda envelope: {"execution": envelope["identity"]}
        prompts = []
        def provider(model, prompt, **kwargs):
            prompts.append(prompt)
            raise bot_runner.providers.ProviderTerminal("deliberately stop after observing selected task")
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _cfg(tmp, name="analytics_engineer", context={
                "ANALYTICS_CONTEXT": {"command": [sys.executable, "-c", 'print(\'{"backlog_count":0,"selected":null}\')'],
                                      "timeout_s": 5},
            }, gate={"context": "ANALYTICS_CONTEXT", "path": "backlog_count", "value": 0,
                     "reason_code": "no_analytics_work"})
            pathlib.Path(cfg["dir"], "prompt.md").write_text("parent {{ANALYTICS_CONTEXT}}")
            with mock.patch.object(bot_runner, "dashboard_model", return_value={"_dashboard_managed": True,
                    "provider": "command", "model": "test", "alias": "test", "timeout_s": 30}), \
                 mock.patch.object(bot_runner.providers, "get_provider", return_value=provider):
                result = bot_runner.run(cfg, client=control, models_config=_models(tmp),
                    identity={**IDENTITY, "dag_id": "bot__analytics_engineer", "run_id": "planning__github"})
        self.assertEqual("failed", result.outcome)
        self.assertEqual(1, len(prompts))
        selected = json.loads(prompts[0].split("\n\nReturn one JSON")[0].removeprefix("parent "))
        self.assertEqual("github-parent", selected["selected"]["parent_task_id"])

    def test_unavailable_workload_fails_closed_and_saves_failure(self):
        result, control, context = self.run_bot("source_discovery", error=ControlPlaneError("request_failed"))
        self.assertEqual(("failed", "transient", "request_failed"),
                         (result.outcome, result.retry_class, result.reason_code))
        context.assert_not_called()
        self.assertEqual([], control.submit_run.call_args.args[0]["attempts"])

    def test_dry_and_ephemeral_runs_do_not_consult_live_intake(self):
        for option in ("dry_run", "ephemeral"):
            _, control, context = self.run_bot("source_discovery", **{option: True})
            control.workload.assert_not_called()
            control.submit_run.assert_not_called()
            context.assert_called_once()

    def test_client_uses_workload_endpoint_and_rejects_malformed_permission(self):
        client = object.__new__(DashboardClient)
        for value in ({"allowed": False}, {"allowed": True}, {}, {"allowed": "false"}, None):
            with self.subTest(value=value), mock.patch.object(client, "_request", return_value=value) as request:
                if isinstance(value, dict) and type(value.get("allowed")) is bool:
                    self.assertEqual(value, client.workload())
                else:
                    with self.assertRaises(ControlPlaneError):
                        client.workload()
                request.assert_called_once_with("GET", "workload")


if __name__ == "__main__":
    unittest.main()
