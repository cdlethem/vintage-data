from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard import execution, maintenance, repository_ops, review
from airflow.providers.vintage.bot_dashboard.artifacts import put_artifact
from airflow.providers.vintage.bot_dashboard.git_provider import GitHubProvider, GitLabProvider, RepositoryConfig
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Task, metadata
from airflow.providers.vintage.bot_dashboard.projection import persist_run_envelope
from airflow.providers.vintage.bot_dashboard.report_schemas import TaskProposalV1
from airflow.providers.vintage.bot_dashboard.service import (
    add_event_items,
    assign_task,
    start_task,
    transition_task,
)


UTC = timezone.utc
SECRET = "provider-token-must-never-escape"


def _git(*args: str, cwd: Path) -> str:
    result = subprocess.run(
        ["git", *args], cwd=cwd, text=True, capture_output=True, check=True,
        env={"PATH": "/usr/bin:/bin", "GIT_AUTHOR_NAME": "fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid", "GIT_COMMITTER_NAME": "fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid"},
    )
    return result.stdout.strip()


class FakeProviderHTTP:
    """HTTP-shaped GitHub/GitLab responses, backed by deterministic in-memory state."""

    def __init__(self, provider: str):
        self.provider = provider
        self.created = False
        self.draft = True
        self.merged = False
        self.head_sha = "0" * 40
        self.comments: list[dict] = []
        self.calls: list[tuple[str, str]] = []
        self.number = 7 if provider == "github" else 11
        self.timeout = False

    def provider_object(self, config: RepositoryConfig):
        adapter = GitHubProvider(config) if self.provider == "github" else GitLabProvider(config)
        adapter._request = self.request  # type: ignore[method-assign]
        return adapter

    def request(self, method: str, path: str, *, payload: dict | None = None):
        if self.timeout and method == "GET" and ("/pulls/" in path or "/merge_requests/" in path):
            raise TimeoutError("fake provider timeout")
        self.calls.append((method, path))
        if "/comments" in path or "/notes" in path:
            if method == "GET":
                return list(self.comments)
            if method in {"POST", "PATCH", "PUT"}:
                body = (payload or {}).get("body", "")
                if method in {"PATCH", "PUT"}:
                    self.comments[0]["body"] = body
                else:
                    self.comments.append({"id": len(self.comments) + 1, "body": body})
                return self.comments[-1]
        if method == "GET" and ("pulls?" in path or "merge_requests?" in path):
            return [self.raw()] if self.created else []
        if method == "POST" and ("/pulls" in path or "/merge_requests" in path):
            self.created = True
            return self.raw()
        if method == "GET":
            return self.raw()
        raise AssertionError(f"unexpected provider request: {method} {path}")

    def raw(self) -> dict:
        if self.provider == "github":
            return {
                "number": self.number, "html_url": f"https://fake.invalid/pr/{self.number}",
                "state": "open", "merged": self.merged, "draft": self.draft,
                "head": {"sha": self.head_sha, "ref": self.branch},
                "base": {"ref": "main"}, "user": {"id": 4242},
            }
        return {
            "iid": self.number, "web_url": f"https://fake.invalid/mr/{self.number}",
            "state": "merged" if self.merged else "opened", "draft": self.draft,
            "sha": self.head_sha, "source_branch": self.branch,
            "target_branch": "main", "author": {"id": 4242},
        }

    @property
    def branch(self) -> str:
        return self._branch

    @branch.setter
    def branch(self, value: str) -> None:
        self._branch = value


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="bot-dashboard-lifecycle-")
        self.root = Path(self.tmp.name)
        self.engine = create_engine(f"sqlite:///{self.root / 'dashboard.sqlite'}")
        metadata.create_all(self.engine)
        self.remote = self.root / "remote.git"
        self.shared = self.root / "shared"
        _git("init", "--bare", str(self.remote), cwd=self.root)
        _git("clone", str(self.remote), str(self.shared), cwd=self.root)
        (self.shared / "allowed.txt").write_text("base\n")
        _git("add", "allowed.txt", cwd=self.shared)
        _git("commit", "-m", "base", cwd=self.shared)
        _git("branch", "-M", "main", cwd=self.shared)
        _git("push", "origin", "main", cwd=self.shared)
        self.base_sha = _git("rev-parse", "HEAD", cwd=self.shared)
        self._run_counter = 0
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir(mode=0o700)

    def _reset_db(self):
        metadata.drop_all(self.engine)
        metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.tmp.cleanup()

    def _config(self, provider: str) -> RepositoryConfig:
        return RepositoryConfig(
            provider=provider, project="fixture/project", api_base_url="https://fake.invalid/api",
            clone_url=str(self.remote), base_branch="main", allowed_path_globs=("*.txt",),
            denied_path_globs=(".git/**",), max_changed_files=1, max_diff_bytes=100_000,
            service_account_id="4242", token=SECRET,
        )

    def _proposal(self, *, path: str = "allowed.txt") -> dict:
        return TaskProposalV1(
            recommendation_key="one-change", title="Update fixture", category="reliability", priority=1,
            planned_resolution="Update the allowed fixture file", why_now="The fixture is stale",
            expected_benefit="Deterministic lifecycle proof", risk="Low", rollback="Revert the commit",
            verification_commands=[["python", "-c", "print('ok')"]], allowed_path_globs=[path],
            resource_keys=["fixture"], follow_up_bots=[], suggested_executor="junior",
            reviewer_required=True, evidence=[{"kind": "run", "reference": "specialist/run", "summary": "fixture"}],
        ).model_dump(mode="json")

    def _envelope(self, bot: str, dag_id: str, run_id: str, payload_schema: str | None, payload: dict | None, *, reason="ok", try_number=1, outcome="succeeded", retry_class="none") -> dict:
        now = datetime.now(UTC)
        return {
            "envelope_version": 1,
            "identity": {"bot": bot, "dag_id": dag_id, "run_id": run_id, "task_id": "run", "map_index": -1, "try_number": try_number},
            "timing": {"started_at": now.isoformat(), "finished_at": (now + timedelta(milliseconds=5)).isoformat(), "deadline_at": (now + timedelta(minutes=10)).isoformat(), "duration_ms": 5},
            "outcome": outcome, "retry_class": retry_class, "reason_code": reason, "failure": None,
            "selected_model": "fixture", "attempts": [], "context": {"sha256": hashlib.sha256(b"{}").hexdigest(), "byte_count": 2, "build_ms": 0},
            "payload_schema": payload_schema, "payload": payload,
        }

    def _patch_and_manifest(self, execution_row: Execution, *, changed_path="allowed.txt", content="changed\n"):
        sandbox = self.root / f"sandbox-{execution_row.execution_id[:8]}"
        _git("clone", str(self.remote), str(sandbox), cwd=self.root)
        _git("checkout", "main", cwd=sandbox)
        (sandbox / changed_path).write_text(content)
        _git("add", changed_path, cwd=sandbox)
        patch = subprocess.run(["git", "diff", "--binary", "--cached"], cwd=sandbox, capture_output=True, check=True).stdout
        patch_sha = hashlib.sha256(patch).hexdigest()
        manifest = {
            "manifest_version": 1, "task_id": str(execution_row.task_id), "execution_id": execution_row.execution_id,
            "revision": execution_row.revision, "base_sha": self.base_sha, "changed_paths": [changed_path],
            "patch_sha256": patch_sha, "patch_bytes": len(patch), "source_report_reference": "source_vetting/run",
            "checks": [{"name": "fixture", "argv": ["python", "-c", "print('ok')"], "exit_code": 0}],
        }
        return patch, patch_sha, manifest

    def _begin(self, provider: str):
        config = self._config(provider)
        fake = FakeProviderHTTP(provider)
        fake.branch = ""
        patches = ExitStack()
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.artifacts.artifact_root", return_value=self.artifacts))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.execution.require_executor_preconditions"))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.execution.load_repository_config", return_value=config, create=True))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.repository_ops.load_repository_config", return_value=config))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.git_provider.load_repository_config", return_value=config))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.repository_ops.get_provider", side_effect=lambda _config=None: fake.provider_object(config)))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.review.get_provider", side_effect=lambda _config=None: fake.provider_object(config)))
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.maintenance.get_provider", side_effect=lambda _config=None: fake.provider_object(config)))
        original_git = repository_ops._git
        def capture_git(root, cfg, *args, **kwargs):
            result = original_git(root, cfg, *args, **kwargs)
            if args == ("rev-parse", "HEAD"):
                fake.head_sha = result
            if args == ("checkout", "-b", fake.branch):
                fake.branch = args[-1]
            return result
        patches.enter_context(patch("airflow.providers.vintage.bot_dashboard.repository_ops._git", side_effect=capture_git))
        return config, fake, patches

    def _create_task(self, session: Session):
        self._run_counter += 1
        envelope = self._envelope("source_vetting", "bot__source_vetting", f"specialist-run-{self._run_counter}", "source_vetting_v2", {
            "schema_version": 2, "agent": "source_vetting", "status": "ok",
            "decisions": [{"slug": "fixture", "decision": "recommended", "reason": "ready", "task_proposal": self._proposal()}],
            "summary": "one proposal",
        })
        persist_run_envelope(session, envelope)
        session.flush()
        task = session.scalars(select(Task).where(Task.source_bot == "source_vetting").order_by(Task.created_at.desc())).first()
        transition_task(session, str(task.id), version=task.version, actor_id="human", to_state="accepted")
        assign_task(session, str(task.id), version=task.version, actor_id="human", actor_name="Human", kind="bot", profile="junior")
        start_task(session, str(task.id), version=task.version, actor_id="human", idempotency_key="human-start")
        start_task(session, str(task.id), version=task.version, actor_id="human", idempotency_key="human-start")
        session.commit()
        return task

    def test_happy_path_is_immutable_and_provider_neutral(self):
        for provider in ("github", "gitlab"):
            self._reset_db()
            with self.subTest(provider=provider):
                config, fake, patches = self._begin(provider)
                with patches:
                    with Session(self.engine) as session:
                        task = self._create_task(session)
                        row = session.scalar(select(Execution).where(Execution.task_id == task.id))
                        self.assertIsNotNone(row)
                        row = row  # narrow for type checkers
                        dispatch = execution.claim_pending(session)
                        self.assertEqual(1, len(dispatch))
                        admission = execution.claim_run(session, dag_id="bot__task_executor", run_id=row.target_run_id, conf_value=dispatch[0]["conf"], kind="executor", deadline_at=datetime.now(UTC) + timedelta(minutes=5))
                        self.assertEqual(2, admission["protocol_version"])
                        patch_bytes, patch_sha, manifest = self._patch_and_manifest(row)
                        put_artifact(session, kind="patch", content=patch_bytes, expected_sha256=patch_sha, owner_execution_id=row.execution_id)
                        fake.branch = f"bot-dashboard/{task.id}/{row.sequence}-r{row.revision}"
                        published = repository_ops.publish_execution_change(session, row, task, task.revisions[-1], {"status": "ok", "patch_sha256": patch_sha, "changed_paths": ["allowed.txt"], "verification_manifest": manifest, "report_sha256": "a" * 64})
                        self.assertEqual(7 if provider == "github" else 11, published["pr_number"])
                        replay = repository_ops.publish_execution_change(session, row, task, task.revisions[-1], {"status": "ok", "patch_sha256": patch_sha, "changed_paths": ["allowed.txt"], "verification_manifest": manifest, "base_sha": row.base_sha, "trusted_head_sha": row.trusted_head_sha})
                        self.assertEqual(published["pr_number"], replay["pr_number"])
                        run_id = row.target_run_id
                        report = self._envelope("task_executor", "bot__task_executor", run_id, "task_executor_v2", {"schema_version": 2, "agent": "task_executor", "status": "ok", "task_id": str(task.id), "summary": "changed one file", "changed_paths": ["allowed.txt"], "verification": [{"name": "fixture", "argv": ["python", "-c", "print('ok')"], "exit_code": 0, "observed": "ok", "observation_sha256": hashlib.sha256(b"ok").hexdigest()}], "blockers": []})
                        executor_projection = persist_run_envelope(session, report)
                        execution.finalize_run(session, dag_id="bot__task_executor", run_id=run_id, kind="executor", result={"result_artifact_sha256": executor_projection["report_sha256"]})
                        session.flush()
                        row = session.scalar(select(Execution).where(Execution.task_id == task.id))
                        dispatch = execution.claim_pending(session)
                        reviewer_run = dispatch[0]["conf"]
                        review_admission = execution.claim_run(session, dag_id="bot__pr_reviewer", run_id=row.target_run_id, conf_value=reviewer_run, kind="pr_reviewer", deadline_at=datetime.now(UTC) + timedelta(minutes=4))
                        self.assertEqual("pr_reviewer", review_admission["kind"])
                        review_report = self._envelope("pr_reviewer", "bot__pr_reviewer", row.target_run_id, "pr_reviewer_v2", {"schema_version": 2, "agent": "pr_reviewer", "status": "ok", "task_id": str(task.id), "verdict": "approved", "summary": "looks good", "comments": [{"body": "Reviewed fixture", "path": "allowed.txt", "line": 1}], "verification": ["fixture passed"]})
                        persist_run_envelope(session, review_report)
                        execution.finalize_run(session, dag_id="bot__pr_reviewer", run_id=row.target_run_id, kind="pr_reviewer", result={})
                        self.assertEqual(1, len(fake.comments))
                        marker = f"bot-dashboard-review:{row.execution_id}:r{row.revision}"
                        self.assertIn(marker, fake.comments[0]["body"])
                        fake.draft = False
                        self.assertEqual(1, maintenance.sync_provider(session)["changed"])
                        task = session.get(Task, task.id)
                        self.assertEqual("ready", task.state)
                        row = session.scalar(select(Execution).where(Execution.task_id == task.id))
                        row.synced_at = None
                        fake.draft = False
                        self.assertEqual(0, maintenance.sync_provider(session)["changed"])
                        row.synced_at = None
                        fake.merged = True
                        self.assertEqual(1, maintenance.sync_provider(session)["changed"])
                        row = session.scalar(select(Execution).where(Execution.task_id == task.id))
                        self.assertEqual("merged", row.provider_state["state"])
                        task = session.get(Task, task.id)
                        comment_result = add_event_items(session, str(task.id), version=task.version, actor_id="human", event_type="comment_added", items=["Human verification comment"])
                        evidence_result = add_event_items(session, str(task.id), version=comment_result["version"], actor_id="human", event_type="evidence_added", items=[{"label": "merge", "url": "https://evidence.invalid/merge"}])
                        transition_task(session, str(task.id), version=evidence_result["version"], actor_id="human", to_state="completed", reason="Human verified merge")
                        session.commit()
                        events = session.scalars(select(Event).where(Event.task_id == task.id).order_by(Event.sequence)).all()
                        self.assertEqual("completed", session.get(Task, task.id).state)
                        self.assertEqual(1, session.query(Execution).filter_by(task_id=task.id).count())
                        self.assertEqual(1, len([item for item in fake.comments if marker in item["body"]]))
                        self.assertEqual(sorted(item.sequence for item in events), [item.sequence for item in events])
                        self.assertNotIn(SECRET, json.dumps([item.payload for item in events]))
                        self.assertNotIn(SECRET, (self.artifacts / patch_sha[:2] / patch_sha).read_text(errors="ignore"))
                        self.assertEqual(self.base_sha, _git("rev-parse", "HEAD", cwd=self.shared))
                        self.assertEqual("base\n", (self.shared / "allowed.txt").read_text())

    def test_adverse_cases_are_bounded_and_idempotent(self):
        scenarios = ("forbidden_path", "oversized_diff", "changed_base_sha", "changed_pr_head", "no_change", "changes_requested", "provider_timeout", "duplicate")
        for scenario in scenarios:
            self._reset_db()
            with self.subTest(scenario=scenario):
                config, fake, patches = self._begin("github")
                with patches:
                    with Session(self.engine) as session:
                        task = self._create_task(session)
                        row = session.scalar(select(Execution).where(Execution.task_id == task.id))
                        dispatch = execution.claim_pending(session)
                        execution.claim_run(session, dag_id="bot__task_executor", run_id=row.target_run_id, conf_value=dispatch[0]["conf"], kind="executor", deadline_at=datetime.now(UTC) + timedelta(minutes=5))
                        deadline = row.executor_deadline_at
                        if scenario == "no_change":
                            payload = {"schema_version": 2, "agent": "task_executor", "status": "no_change", "task_id": str(task.id), "summary": "already correct", "changed_paths": [], "verification": [], "blockers": []}
                            projection = persist_run_envelope(session, self._envelope("task_executor", "bot__task_executor", row.target_run_id, "task_executor_v2", payload))
                            execution.finalize_run(session, dag_id="bot__task_executor", run_id=row.target_run_id, kind="executor", result={"result_artifact_sha256": projection["report_sha256"]})
                            self.assertEqual("ready", session.get(Task, task.id).state)
                            self.assertIsNone(row.pr_number)
                        elif scenario in {"forbidden_path", "oversized_diff", "changed_base_sha"}:
                            changed = "outside.py" if scenario == "forbidden_path" else "allowed.txt"
                            patch_bytes, patch_sha, manifest = self._patch_and_manifest(row, changed_path=changed, content="x" * (100_001 if scenario == "oversized_diff" else 2) + "\n")
                            put_artifact(session, kind="patch", content=patch_bytes, expected_sha256=patch_sha, owner_execution_id=row.execution_id)
                            if scenario == "changed_base_sha":
                                other = self.root / "other.git"
                                _git("init", "--bare", str(other), cwd=self.root)
                                changed_config = RepositoryConfig(**{**config.__dict__, "clone_url": str(other)})
                                with patch("airflow.providers.vintage.bot_dashboard.repository_ops.load_repository_config", return_value=changed_config):
                                    with self.assertRaises(Exception):
                                        repository_ops.publish_execution_change(session, row, task, task.revisions[-1], {"status": "ok", "patch_sha256": patch_sha, "changed_paths": [changed], "verification_manifest": manifest})
                            else:
                                with self.assertRaises(Exception):
                                    repository_ops.publish_execution_change(session, row, task, task.revisions[-1], {"status": "ok", "patch_sha256": patch_sha, "changed_paths": [changed], "verification_manifest": manifest})
                            self.assertIsNone(row.pr_number)
                            self.assertEqual(deadline, row.executor_deadline_at)
                        elif scenario == "duplicate":
                            self.assertEqual(0, len(execution.claim_pending(session)))
                            self.assertEqual(deadline, row.executor_deadline_at)
                            self.assertEqual(1, session.query(Execution).filter_by(task_id=task.id).count())
                        else:
                            patch_bytes, patch_sha, manifest = self._patch_and_manifest(row)
                            put_artifact(session, kind="patch", content=patch_bytes, expected_sha256=patch_sha, owner_execution_id=row.execution_id)
                            fake.branch = f"bot-dashboard/{task.id}/{row.sequence}-r{row.revision}"
                            repository_ops.publish_execution_change(session, row, task, task.revisions[-1], {"status": "ok", "patch_sha256": patch_sha, "changed_paths": ["allowed.txt"], "verification_manifest": manifest, "report_sha256": "a" * 64})
                            if scenario == "changed_pr_head":
                                fake.head_sha = "f" * 40
                                maintenance.sync_provider(session)
                                self.assertEqual("provider_head_drift", row.terminal_reason_code)
                            elif scenario == "provider_timeout":
                                fake.timeout = True
                                with self.assertRaises(Exception):
                                    maintenance.sync_provider(session)
                                fake.timeout = False
                                self.assertIsNone(row.merged_at)
                            else:
                                review_payload = {"schema_version": 2, "agent": "pr_reviewer", "status": "ok", "task_id": str(task.id), "verdict": "changes_requested", "summary": "needs work", "comments": [{"body": "Please revise", "path": "allowed.txt", "line": 1}], "verification": []}
                                executor_payload = {"schema_version": 2, "agent": "task_executor", "status": "ok", "task_id": str(task.id), "summary": "changed one file", "changed_paths": ["allowed.txt"], "verification": [], "blockers": []}
                                persist_run_envelope(session, self._envelope("task_executor", "bot__task_executor", row.target_run_id, "task_executor_v2", executor_payload))
                                execution.finalize_run(session, dag_id="bot__task_executor", run_id=row.target_run_id, kind="executor", result={})
                                session.flush()
                                row = session.scalar(select(Execution).where(Execution.task_id == task.id))
                                reviewer_dispatch = execution.claim_pending(session)[0]
                                execution.claim_run(session, dag_id="bot__pr_reviewer", run_id=row.target_run_id, conf_value=reviewer_dispatch["conf"], kind="pr_reviewer", deadline_at=datetime.now(UTC) + timedelta(minutes=4))
                                persist_run_envelope(session, self._envelope("pr_reviewer", "bot__pr_reviewer", row.target_run_id, "pr_reviewer_v2", review_payload))
                                execution.finalize_run(session, dag_id="bot__pr_reviewer", run_id=row.target_run_id, kind="pr_reviewer", result={})
                                self.assertEqual("changes_requested", row.review_verdict)
                                self.assertEqual(1, len(fake.comments))
                                self.assertEqual("in_review", session.get(Task, task.id).state)
                        self.assertEqual(self.base_sha, _git("rev-parse", "HEAD", cwd=self.shared))
                        self.assertNotIn(SECRET, str(row.__dict__))
                        session.rollback()


if __name__ == "__main__":
    unittest.main()
