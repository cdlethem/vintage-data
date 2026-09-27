from datetime import timedelta
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import hashlib
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import select
from sqlalchemy.orm import Session
import test_execution_retry as fixtures
from airflow.providers.vintage.bot_dashboard import service
from airflow.providers.vintage.bot_dashboard.models import Execution, ValidationGate, utcnow
from airflow.providers.vintage.bot_dashboard.revision_seed import capture_seed, get_seed
from bots.admitted_runner import _initialize_baseline, _git, _apply_revision_seed


class RevisionSeedTest(unittest.TestCase):
    setUp = fixtures.ExecutionRetryTest.setUp
    tearDown = fixtures.ExecutionRetryTest.tearDown
    fixture = fixtures.ExecutionRetryTest.fixture

    def test_expired_optional_reports_do_not_block_reproducible_repair(self):
        with Session(self.engine) as session:
            _, row = self.fixture(session)
            row.base_sha = "a" * 40
            row.source_artifact_sha256 = "b" * 64
            row.patch_sha256 = "c" * 64
            row.executor_report_sha256 = "d" * 64
            row.review_report_sha256 = "e" * 64
            with patch("airflow.providers.vintage.bot_dashboard.artifacts.read_artifact") as read:
                read.side_effect = lambda _, digest: (_ for _ in ()).throw(FileNotFoundError()) if digest in {"d" * 64, "e" * 64} else None
                seed = capture_seed(session, row)
            self.assertEqual("b" * 64, seed["source_artifact_sha256"])
            self.assertEqual("c" * 64, seed["patch_sha256"])
            self.assertIsNone(seed["executor_report_sha256"])
            self.assertIsNone(seed["review_report_sha256"])
    def test_revision_and_retry_preserve_candidate_without_carrying_approval(self):
        with Session(self.engine) as session:
            task, row = self.fixture(session)
            task.state = 'in_review'
            row.terminal_at = None
            row.stage = 'reviewed'
            row.review_verdict = 'changes_requested'
            row.base_sha = 'a' * 40
            row.source_artifact_sha256 = 'b' * 64
            row.patch_sha256 = 'c' * 64
            row.provider = 'github'; row.repository = 'org/repo'; row.target_branch = 'main'
            row.pr_number = 37; row.pr_url = 'https://example.test/pull/37'
            row.trusted_head_sha = 'd' * 40
            old_execution_id = row.execution_id
            service.patch_task(session, str(task.id), version=task.version, actor_id='user',
                               changes={'planned_resolution': 'Retain implementation and fix review defect'})
            session.commit()
            with patch('airflow.providers.vintage.bot_dashboard.artifacts.read_artifact') as read:
                service.start_task(session, str(task.id), version=task.version, actor_id='user',
                                   idempotency_key='revision', revision=True)
            session.commit()
            self.assertEqual(2, read.call_count)
            seed = get_seed(session, row)
            self.assertEqual('c' * 64, seed['patch_sha256'])
            self.assertEqual(old_execution_id, seed['execution_id'])
            self.assertEqual(37, seed['pr_number'])
            self.assertEqual('a' * 40, row.base_sha)
            self.assertEqual('b' * 64, row.source_artifact_sha256)
            self.assertIsNone(row.patch_sha256)
            self.assertIsNone(row.review_verdict)
            self.assertIsNone(row.pr_number)
            self.assertTrue(row.reviewer_required)
            row.terminal_at = utcnow(); row.terminal_reason_code = 'launcher_failed'
            row.dispatch_state = 'terminal'; task.state = 'blocked'; task.blocked_from_state = 'in_progress'
            session.commit()
            service.start_task(session, str(task.id), version=task.version, actor_id='user', idempotency_key='retry-seeded')
            session.commit()
            retry = session.scalar(select(Execution).where(Execution.task_id == task.id).order_by(Execution.sequence.desc()).limit(1))
            self.assertEqual(seed, get_seed(session, retry))
            self.assertEqual(row.source_artifact_sha256, retry.source_artifact_sha256)
            self.assertIsNone(retry.review_verdict)
            from airflow.providers.vintage.bot_dashboard.execution import claim_run
            config = SimpleNamespace(provider='github', project='org/repo', base_branch='main',
                                     allowed_path_globs=['src/**'], denied_path_globs=[], max_changed_files=5, max_diff_bytes=10000)
            with ExitStack() as stack:
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.execution.require_executor_preconditions'))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.git_provider.load_repository_config', return_value=config))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.repository_ops.create_source_artifact', return_value={'sha256': 'b'*64, 'byte_count': 100}))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.model_settings.model_for_role', return_value={'provider_id':'fixture','model':'small','base_url':'https://model.example/v1'}))
                admission = claim_run(session, dag_id='bot__task_executor', run_id=retry.target_run_id,
                                      conf_value={'execution_id': retry.execution_id, 'revision': retry.revision},
                                      kind='executor', deadline_at=utcnow()+timedelta(minutes=10))
            self.assertEqual('c'*64, admission['seed_patch_sha256'])
            self.assertEqual('a'*40, admission['base_sha'])
            self.assertIsNone(admission['patch_sha256'])
            resolution = admission['task']['planned_resolution']
            self.assertIn('Autopilot owns this revision repair', resolution)
            self.assertIn('reconcile every rejected hunk', resolution)
            self.assertIn('There is no external or human blocker', resolution)
            self.assertIn('Retain implementation and fix review defect', resolution)

    def test_failed_live_merge_gate_can_revise_reviewed_ready_candidate(self):
        from airflow.providers.vintage.bot_dashboard.autopilot import _actions
        from airflow.providers.vintage.bot_dashboard.service import PreconditionFailed, get_task
        with Session(self.engine) as session:
            task, row = self.fixture(session)
            task.state = "ready"
            row.terminal_at = None
            row.stage = "reviewed"
            row.base_sha = "a" * 40
            row.source_artifact_sha256 = "b" * 64
            row.patch_sha256 = "c" * 64
            row.provider = "github"
            row.repository = "org/repo"
            row.target_branch = "main"
            row.pr_number = 37
            row.pr_url = "https://example.test/pull/37"
            row.trusted_head_sha = "d" * 40
            gate = ValidationGate(
                task_id=task.id, gate_key="source", stage="merge",
                recipe="trusted_workflow_check", owner="validation-service",
                required_capability="github-actions-readonly", subject=row.trusted_head_sha,
                recheck_condition="Repair source parsing and compare the new candidate head",
                required=True, status="failed",
                evidence={"label": "Live source", "observation": "HTML pair schema mismatch"},
            )
            session.add(gate)
            session.commit()
            self.assertNotIn("revise", _actions(get_task(session, str(task.id))))
            with patch("airflow.providers.vintage.bot_dashboard.artifacts.read_artifact"):
                with self.assertRaisesRegex(PreconditionFailed, "newer task recommendation revision"):
                    service.start_task(session, str(task.id), version=task.version,
                                       actor_id="executive", idempotency_key="premature", revision=True)
            service.patch_task(session, str(task.id), version=task.version, actor_id="executive",
                               changes={"planned_resolution": "Repair the existing HTML pair parser"})
            session.commit()
            session.expire_all()
            self.assertIn("revise", _actions(get_task(session, str(task.id))))
            with patch("airflow.providers.vintage.bot_dashboard.artifacts.read_artifact"):
                service.start_task(session, str(task.id), version=task.version,
                                   actor_id="executive", idempotency_key="repair", revision=True)
            self.assertEqual("pending_candidate", gate.subject)
            self.assertEqual("pending", gate.status)
            self.assertIsNone(row.review_verdict)
            self.assertIsNone(row.trusted_head_sha)
            from airflow.providers.vintage.bot_dashboard.execution import claim_run
            config = SimpleNamespace(provider="github", project="org/repo", base_branch="main",
                                     allowed_path_globs=["src/**"], denied_path_globs=[],
                                     max_changed_files=5, max_diff_bytes=10000)
            with ExitStack() as stack:
                stack.enter_context(patch("airflow.providers.vintage.bot_dashboard.execution.require_executor_preconditions"))
                stack.enter_context(patch("airflow.providers.vintage.bot_dashboard.git_provider.load_repository_config", return_value=config))
                stack.enter_context(patch("airflow.providers.vintage.bot_dashboard.repository_ops.create_source_artifact", return_value={"sha256": "b" * 64, "byte_count": 100}))
                stack.enter_context(patch("airflow.providers.vintage.bot_dashboard.model_settings.model_for_role", return_value={"provider_id": "fixture", "model": "small", "base_url": "https://model.example/v1"}))
                claim_run(session, dag_id="bot__task_executor", run_id=row.target_run_id,
                          conf_value={"execution_id": row.execution_id, "revision": row.revision},
                          kind="executor", deadline_at=utcnow() + timedelta(minutes=10))
            self.assertEqual("in_progress", task.state)

    def test_pending_auto_check_does_not_trap_a_superseded_candidate(self):
        from airflow.providers.vintage.bot_dashboard.autopilot import _actions
        from airflow.providers.vintage.bot_dashboard.service import get_task
        with Session(self.engine) as session:
            task, row = self.fixture(session)
            task.state = "ready"
            row.terminal_at = None
            row.stage = "reviewed"
            row.base_sha = "a" * 40
            row.source_artifact_sha256 = "b" * 64
            row.patch_sha256 = "c" * 64
            row.provider = "github"
            row.repository = "org/repo"
            row.target_branch = "main"
            row.pr_number = 37
            row.trusted_head_sha = "d" * 40
            gate = ValidationGate(
                task_id=task.id, gate_key="live", stage="merge",
                recipe="trusted_workflow_check", owner="validation-service",
                required_capability="github-actions-readonly", subject=row.trusted_head_sha,
                recheck_condition="Validate revised source head", required=True, status="pending",
            )
            session.add(gate)
            service.patch_task(session, str(task.id), version=task.version, actor_id="executive",
                               changes={"planned_resolution": "Repair source layout before live comparison"})
            session.commit()
            session.expire_all()
            self.assertIn("revise", _actions(get_task(session, str(task.id))))
            with patch("airflow.providers.vintage.bot_dashboard.artifacts.read_artifact"):
                service.start_task(session, str(task.id), version=task.version, actor_id="executive",
                                   idempotency_key="repair-pending", revision=True)
            self.assertEqual("pending_candidate", gate.subject)
            self.assertEqual("pending", gate.status)
            self.assertIsNone(row.trusted_head_sha)

    def test_seed_retains_prior_tests_and_produces_cumulative_patch(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp); work = root / 'work'; work.mkdir()
            (work / 'base.txt').write_text('base\n')
            metadata = _initialize_baseline(root, work)
            (work / 'extract.py').write_text('value = 1\n')
            (work / 'test_extract.py').write_text('assert value == 1\n')
            _git(work, metadata, 'add', '-A')
            seed = _git(work, metadata, 'diff', '--cached', '--binary', 'HEAD', binary=True)
            _git(work, metadata, 'reset', '--hard', 'HEAD')
            client = Mock(); client.get_artifact.return_value = seed
            admission = {'seed_patch_sha256': hashlib.sha256(seed).hexdigest()}
            _apply_revision_seed(admission, client, work, metadata, lambda code, _: ValueError(code))
            self.assertEqual('assert value == 1\n', (work / 'test_extract.py').read_text())
            (work / 'extract.py').write_text('value = 2\n')
            _git(work, metadata, 'add', '-A')
            combined = _git(work, metadata, 'diff', '--cached', '--binary', 'HEAD', binary=True)
            self.assertIn(b'+value = 2', combined)
            self.assertIn(b'+assert value == 1', combined)
            _git(work, metadata, 'reset', '--hard', 'HEAD')
            _git(work, metadata, 'apply', '--binary', '-', binary=True, input_data=combined)
            self.assertEqual('value = 2\n', (work / 'extract.py').read_text())
            client.get_artifact.return_value = b'corrupt'
            with self.assertRaisesRegex(ValueError, 'revision_seed_digest_invalid'):
                _apply_revision_seed(admission, client, work, metadata, lambda code, _: ValueError(code))

    def test_seed_conflict_is_exposed_inside_the_confined_worktree_for_repair(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"; old.mkdir()
            (old / "extract.py").write_text("value = 'base'\n")
            old_metadata = _initialize_baseline(root, old)
            (old / "extract.py").write_text("value = 'candidate'\n")
            _git(old, old_metadata, "add", "-A")
            seed = _git(old, old_metadata, "diff", "--cached", "--binary", "HEAD", binary=True)

            current = root / "current"; current.mkdir()
            (current / "extract.py").write_text("value = 'upstream'\n")
            metadata = _initialize_baseline(root, current)
            client = Mock(); client.get_artifact.return_value = seed
            admission = {"seed_patch_sha256": hashlib.sha256(seed).hexdigest()}
            _apply_revision_seed(admission, client, current, metadata, lambda code, _: ValueError(code))
            reject = current / "extract.py.rej"
            self.assertTrue(reject.is_file())
            self.assertIn("candidate", reject.read_text())

            (current / "extract.py").write_text("value = 'repaired'\n")
            reject.unlink()
            _git(current, metadata, "add", "-A")
            repaired = _git(current, metadata, "diff", "--cached", "--binary", "HEAD", binary=True)
            self.assertIn(b"+value = 'repaired'", repaired)

    def test_deleted_upstream_seed_file_becomes_scoped_reject_evidence(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"; old.mkdir()
            (old / "obsolete.txt").write_text("candidate\n")
            old_metadata = _initialize_baseline(root, old)
            (old / "obsolete.txt").unlink()
            _git(old, old_metadata, "add", "-A")
            seed = _git(old, old_metadata, "diff", "--cached", "--binary", "HEAD", binary=True)

            current = root / "current"; current.mkdir()
            metadata = _initialize_baseline(root, current)
            client = Mock(); client.get_artifact.return_value = seed
            admission = {
                "seed_patch_sha256": hashlib.sha256(seed).hexdigest(),
                "task": {"allowed_path_globs": ["*.txt"]},
                "repository_policy": {
                    "allowed_path_globs": ["*.txt"], "denied_path_globs": [],
                    "max_changed_files": 5, "max_diff_bytes": 10_000,
                },
            }
            _apply_revision_seed(admission, client, current, metadata, lambda code, _: ValueError(code))
            reject = current / "obsolete.txt.rej"
            self.assertTrue(reject.is_file())
            self.assertIn(b"deleted file mode", reject.read_bytes())
