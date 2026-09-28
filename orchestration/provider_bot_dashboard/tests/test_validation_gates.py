import unittest
from unittest.mock import patch
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Task, ValidationGate, metadata
from airflow.providers.vintage.bot_dashboard.service import (
    PreconditionFailed,
    create_manual_task,
    convert_manual_validation_gate,
    create_validation_gate,
    record_validation_gate,
    require_validation_gates,
    patch_task,
)


class ValidationGateTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        self.session = Session(self.engine)
        self.task = create_manual_task(
            self.session, title="Validated work", category="other", priority=1,
            planned_resolution="Make and validate the change", actor_id="owner",
            actor_name="Owner",
        )

    def tearDown(self):
        self.session.close(); self.engine.dispose()

    def test_exact_subject_evidence_controls_the_stage(self):
        gate = create_validation_gate(
            self.session, self.task["id"], version=self.task["version"], actor_id="owner",
            value={
                "gate_key": "live-smoke", "stage": "merge", "recipe": "manual",
                "owner": "operator", "required_capability": "bounded-network",
                "subject": "a" * 40, "dependencies": [],
                "recheck_condition": "candidate head changes", "required": True,
            },
        )
        with self.assertRaisesRegex(PreconditionFailed, "incomplete"):
            require_validation_gates(self.session, uuid.UUID(self.task["id"]), "merge")
        with self.assertRaisesRegex(PreconditionFailed, "different subject"):
            record_validation_gate(
                self.session, gate["id"], version=gate["version"], actor_id="operator",
                status="passed", subject="b" * 40,
                evidence={"label": "Smoke", "observation": "HTTP 200"},
            )
        passed = record_validation_gate(
            self.session, gate["id"], version=gate["version"], actor_id="operator",
            status="passed", subject="a" * 40,
            evidence={"label": "Smoke", "observation": "HTTP 200 with expected schema"},
        )
        self.assertEqual("passed", passed["status"])
        require_validation_gates(self.session, uuid.UUID(self.task["id"]), "merge")

    def test_executive_cannot_add_manual_only_gate_or_weaken_existing_requirement(self):
        gate = {
            "gate_key": "live-smoke", "stage": "merge", "recipe": "manual",
            "owner": "operator", "required_capability": "public-network",
            "subject": "a" * 40, "dependencies": [], "recheck_condition": "head changes",
            "required": True,
        }
        with self.assertRaisesRegex(PreconditionFailed, "executable bot-owned"):
            patch_task(self.session, self.task["id"], version=self.task["version"],
                       actor_id="executive", actor_kind="system", changes={"acceptance_gates": [gate]})
        task = patch_task(self.session, self.task["id"], version=self.task["version"],
                          actor_id="owner", changes={"acceptance_gates": [gate]})
        with self.assertRaises(PreconditionFailed):
            patch_task(self.session, self.task["id"], version=task["version"],
                       actor_id="executive", actor_kind="system",
                       changes={"acceptance_gates": [{**gate, "stage": "completion", "required": False}]})
        stored = self.session.scalar(select(ValidationGate))
        self.assertEqual("merge", stored.stage)
        self.assertTrue(stored.required)
        patch_task(self.session, self.task["id"], version=task["version"],
                   actor_id="executive", actor_kind="system", changes={"acceptance_gates": []})
        with self.assertRaises(PreconditionFailed):
            require_validation_gates(self.session, uuid.UUID(self.task["id"]), "merge")

    def test_failed_manual_gate_can_be_converted_only_to_installed_executable_recipe(self):
        manual = {
            "gate_key": "live-smoke", "stage": "merge", "recipe": "manual",
            "owner": "operator", "required_capability": "public-network",
            "subject": "a" * 40, "dependencies": [], "recheck_condition": "head changes",
            "required": True,
        }
        gate = create_validation_gate(
            self.session, self.task["id"], version=self.task["version"],
            actor_id="owner", value=manual,
        )
        failed = record_validation_gate(
            self.session, gate["id"], version=gate["version"], actor_id="operator",
            status="failed", subject=manual["subject"],
            evidence={"label": "Observed failure", "observation": "Source did not reconcile"},
        )
        recipe_args = {
            "command_id": "reconcile-public-csv",
            "source_url": "https://celestrak.org/SOCRATES/sort-maxProb.csv",
            "expected_status": 200, "required_record_types": ["celestrak_socrates"],
        }
        with patch("airflow.providers.vintage.bot_dashboard.validation_recipes.available_capabilities",
                   return_value=set()):
            with self.assertRaisesRegex(PreconditionFailed, "capability"):
                convert_manual_validation_gate(
                    self.session, gate["id"], version=failed["version"],
                    actor_id="owner", recipe="public_source_reconciliation",
                    required_capability="public-network-readonly", recipe_args=recipe_args,
                )
        self.assertEqual("failed", self.session.get(ValidationGate, uuid.UUID(gate["id"])).status)
        with patch("airflow.providers.vintage.bot_dashboard.validation_recipes.available_capabilities",
                   return_value={"public-network-readonly"}):
            with self.assertRaisesRegex(PreconditionFailed, "public source assertions"):
                convert_manual_validation_gate(
                    self.session, gate["id"], version=failed["version"],
                    actor_id="owner", recipe="public_source_reconciliation",
                    required_capability="public-network-readonly",
                    recipe_args={**recipe_args, "source_url": "https://different.example/source.csv"},
                )
            converted = convert_manual_validation_gate(
                self.session, gate["id"], version=failed["version"], actor_id="owner",
                recipe="public_source_reconciliation",
                required_capability="public-network-readonly", recipe_args=recipe_args,
            )
        self.assertEqual(("pending", "validation-service", True),
                         (converted["status"], converted["owner"], converted["required"]))
        self.assertEqual("Source did not reconcile", converted["evidence"]["observation"])
        event = self.session.scalar(select(Event).where(Event.event_type == "validation_gate_converted"))
        self.assertEqual("failed", event.payload["before"]["status"])
        with self.assertRaisesRegex(PreconditionFailed, "automatic validation lane"):
            record_validation_gate(
                self.session, gate["id"], version=converted["version"], actor_id="operator",
                status="passed", subject=manual["subject"],
                evidence={"label": "Forged pass", "observation": "clicked"},
            )
        with self.assertRaisesRegex(PreconditionFailed, "incomplete"):
            require_validation_gates(self.session, uuid.UUID(self.task["id"]), "merge")
    def test_passed_evidence_for_old_head_cannot_authorize_new_candidate(self):
        identity = uuid.UUID(self.task["id"])
        self.session.add(ValidationGate(task_id=identity, gate_key="live-smoke", stage="merge",
            recipe="manual", owner="operator", required_capability="public-network",
            subject="a" * 40, status="passed", evidence={"observation": "Old candidate passed"},
            recheck_condition="head changes", required=True))
        self.session.add(Execution(task_id=identity, execution_id="e" * 64, sequence=1, revision=1,
            idempotency_key="candidate", target_run_id="candidate", profile="senior",
            trusted_head_sha="b" * 40))
        self.session.flush()
        with self.assertRaisesRegex(PreconditionFailed, "earlier candidate"):
            require_validation_gates(self.session, identity, "merge")


if __name__ == "__main__":
    unittest.main()
