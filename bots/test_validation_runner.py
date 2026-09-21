from __future__ import annotations

import hashlib
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

import validation_runner as runner
from airflow.providers.vintage.bot_dashboard.validation_recipes import ValidationRecipeError, parse_recipe


SUBJECT = "a" * 40


class _Candidates:
    @contextmanager
    def materialize(self, gate):
        candidate = gate.get("candidate", {})
        if candidate.get("head_sha") != gate.get("subject"):
            raise runner.ValidationRunnerError("candidate artifact is not bound to the validation subject")
        with tempfile.TemporaryDirectory() as directory:
            yield Path(directory)


class _Executor:
    def __init__(self, output, capabilities):
        self.output = output
        self._capabilities = capabilities
        self.calls = []

    @property
    def capabilities(self):
        return set(self._capabilities)

    def run(self, command, candidate, environment):
        self.calls.append((command, candidate, environment))
        return runner.CommandResult(0, self.output, False, 1)


class _Credentials:
    def environment(self, capability, names):
        return {}


class _Client:
    def __init__(self, gate):
        self.gate = gate
        self.finished = []

    def claim_validation_gates(self, *, limit, runner_id):
        return [self.gate]

    def start_validation_gate(self, gate_id, **value):
        self.started = {"gate_id": gate_id, **value}
        return {**self.gate, "version": self.gate["version"] + 1}

    def finish_validation_gate(self, gate_id, **value):
        self.finished.append({"gate_id": gate_id, **value})
        return value


def _gate(**changes):
    return {
        "id": "00000000-0000-4000-8000-000000000001", "version": 3,
        "lease_id": "b" * 64, "lease_run_id": "validation__gate__a1", "subject": SUBJECT,
        "recipe": "dag_inspection", "required_capability": "candidate-tree-readonly",
        "recipe_args": {"command_id": "inspect-osv", "dag_id": "osv", "expected_tasks": ["extract"]},
        "candidate": {"head_sha": SUBJECT, "artifact_sha256": "c" * 64},
    } | changes


def _catalog():
    return runner.CommandCatalog.parse({
        "inspect-osv": {"recipe": "dag_inspection", "capability": "candidate-tree-readonly",
                        "argv": ["/usr/bin/true"], "timeout_seconds": 30, "credential_env": []},
    })


class ValidationRecipeTest(unittest.TestCase):
    def test_rejects_unowned_capability_and_unbounded_arguments(self):
        with self.assertRaisesRegex(ValidationRecipeError, "different capability"):
            parse_recipe(recipe="warehouse_check", required_capability="warehouse-write", subject=SUBJECT,
                         recipe_args={"command_id": "warehouse", "expected_relation": "source", "expected_columns": ["id"]})
        with self.assertRaisesRegex(ValidationRecipeError, "supported recipe contract"):
            parse_recipe(recipe="dag_inspection", required_capability="candidate-tree-readonly", subject=SUBJECT,
                         recipe_args={"command_id": "inspect", "dag_id": "osv", "expected_tasks": ["extract"], "argv": ["sh"]})

    def test_catalog_never_accepts_candidate_relative_executable(self):
        with self.assertRaisesRegex(runner.ValidationRunnerError, "absolute"):
            runner.CommandCatalog.parse({"unsafe": {"recipe": "dag_inspection", "capability": "candidate-tree-readonly",
                                                       "argv": ["./candidate-script"], "timeout_seconds": 30}})


class ValidationRunnerTest(unittest.TestCase):
    def test_exact_head_ndjson_assertion_reaches_durable_pass(self):
        worker = runner.ValidationRunner(catalog=_catalog(), candidates=_Candidates(),
                                         executor=_Executor(b'{"kind":"dag","dag_id":"osv","tasks":["extract"]}\n',
                                                            {"candidate-tree-readonly"}), credentials=_Credentials())
        client = _Client(_gate())
        results = worker.run_once(client, runner_id="validation-worker")
        self.assertEqual("passed", results[0]["status"])
        self.assertEqual(4, results[0]["version"])
        self.assertEqual(SUBJECT, results[0]["evidence"]["subject"])
        self.assertEqual(4, client.finished[0]["version"])
        self.assertEqual("validation__gate__a1", client.finished[0]["lease_run_id"])

    def test_changed_candidate_head_fails_with_durable_evidence_without_execution(self):
        executor = _Executor(b'{"kind":"dag","dag_id":"osv","tasks":["extract"]}\n', {"candidate-tree-readonly"})
        worker = runner.ValidationRunner(catalog=_catalog(), candidates=_Candidates(), executor=executor, credentials=_Credentials())
        client = _Client(_gate(candidate={"head_sha": "d" * 40, "artifact_sha256": "c" * 64}))
        worker.run_once(client, runner_id="validation-worker")
        self.assertEqual("failed", client.finished[0]["status"])
        self.assertEqual([], executor.calls)
        self.assertIn("exact subject", client.finished[0]["evidence"]["observation"])

    def test_unavailable_capability_cannot_be_reported_as_passed(self):
        executor = _Executor(b'{"kind":"dag","dag_id":"osv","tasks":["extract"]}\n', set())
        worker = runner.ValidationRunner(catalog=_catalog(), candidates=_Candidates(), executor=executor, credentials=_Credentials())
        client = _Client(_gate())
        worker.run_once(client, runner_id="validation-worker")
        self.assertEqual("failed", client.finished[0]["status"])
        self.assertEqual([], executor.calls)
        self.assertIn("capability", client.finished[0]["evidence"]["reason_code"])


if __name__ == "__main__":
    unittest.main()
