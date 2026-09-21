import unittest
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.models import metadata
from airflow.providers.vintage.bot_dashboard.service import (
    PreconditionFailed,
    create_manual_task,
    create_validation_gate,
    record_validation_gate,
    require_validation_gates,
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
                "gate_key": "live-smoke", "stage": "merge", "recipe": "public_source_smoke",
                "owner": "validation-service", "required_capability": "bounded-network",
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


if __name__ == "__main__":
    unittest.main()
