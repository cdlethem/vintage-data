import unittest
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.providers.vintage.bot_dashboard.models import Execution, Task, ValidationGate, metadata
from airflow.providers.vintage.bot_dashboard.service import (
    PreconditionFailed,
    create_manual_task,
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

    def test_executive_can_add_requirement_but_cannot_weaken_existing_gate(self):
        gate = {
            "gate_key": "live-smoke", "stage": "merge", "recipe": "manual",
            "owner": "operator", "required_capability": "public-network",
            "subject": "a" * 40, "dependencies": [], "recheck_condition": "head changes",
            "required": True,
        }
        task = patch_task(self.session, self.task["id"], version=self.task["version"],
                          actor_id="executive", actor_kind="system", changes={"acceptance_gates": [gate]})
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
