from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import unittest

from airflow.providers.vintage.bot_dashboard.git_provider import (
    GitHubProvider,
    GitLabProvider,
    GitProviderError,
    GitProviderTransientError,
    RepositoryConfig,
    normalize_github,
    normalize_gitlab,
    validate_changed_paths,
)


class FakeGitHub(GitHubProvider):
    def __init__(self, config):
        super().__init__(config)
        self.calls = []
        self.comments = []

    def _request(self, method, path, *, payload=None):
        self.calls.append((method, path, payload))
        if method == "GET" and "commits/" in path:
            return {"sha": "f" * 40}
        if method == "GET" and "comments" in path:
            return [{"id": 7, "body": self.comments[0]}] if self.comments else []
        if method == "POST" and "comments" in path:
            self.comments.append(payload["body"])
        if method == "PATCH" and "comments" in path:
            self.comments[:] = [payload["body"]]
        return {}


class GitProviderTest(unittest.TestCase):
    def setUp(self):
        self.config = RepositoryConfig(provider="github", project="org/repo", api_base_url="https://github.example/api/v3", clone_url="https://github.example/org/repo.git", base_branch="main", allowed_path_globs=("src/**",), denied_path_globs=(".git/**", ".github/workflows/**", ".gitlab-ci.yml"), max_changed_files=2, max_diff_bytes=1000, token="secret", service_account_id="bot-service")

    def test_change_path_policy_rejects_escape_forbidden_and_case_collisions(self):
        validate_changed_paths(self.config, ["src/a.py"], 100)
        for paths in (["../secret"], [".github/workflows/ci.yml"], ["src/A.py", "src/a.py"]):
            with self.subTest(paths=paths), self.assertRaises(GitProviderError):
                validate_changed_paths(self.config, list(paths), 100)
        with self.assertRaises(GitProviderError):
            validate_changed_paths(self.config, ["src/a", "src/b", "src/c"], 100)
        with self.assertRaises(GitProviderError):
            validate_changed_paths(self.config, ["src/a"], 1001)

    def test_provider_normalization_is_bounded_to_trusted_identity_fields(self):
        github = normalize_github({"number": 7, "html_url": "https://github.example/p/7", "merged": False, "state": "open", "draft": True, "head": {"sha": "a" * 40, "ref": "bot-task/id"}, "base": {"ref": "main"}, "user": {"id": 9}, "body": "<script>"})
        self.assertNotIn("body", github)
        self.assertEqual("github", github["provider"])
        gitlab = normalize_gitlab({"iid": 8, "web_url": "https://gitlab.example/m/8", "state": "merged", "draft": False, "title": "Done", "sha": "b" * 40, "source_branch": "bot-task/id", "target_branch": "main", "author": {"id": 10}})
        self.assertEqual("merged", gitlab["state"])
        self.assertEqual("10", gitlab["author_id"])
    def test_dirty_change_and_current_base_are_normalized(self):
        provider = FakeGitHub(self.config)
        change = normalize_github({
            "number": 7, "html_url": "https://github.example/p/7", "merged": False,
            "state": "open", "draft": False, "mergeable": False,
            "mergeable_state": "dirty", "head": {"sha": "a" * 40, "ref": "candidate"},
            "base": {"ref": "main", "sha": "b" * 40}, "user": {"id": 9},
        })
        self.assertEqual("conflicting", change["mergeability"])
        self.assertEqual("b" * 40, change["base_sha"])
        self.assertEqual("f" * 40, provider.read_base_identity())

    def test_review_comment_upsert_uses_one_deterministic_marker(self):
        provider = FakeGitHub(self.config)
        marker = "bot-dashboard-review:execution-1:r1"
        provider.upsert_comment(7, marker, "approved")
        provider.upsert_comment(7, marker, "approved")
        writes = [call for call in provider.calls if call[0] in {"POST", "PATCH"}]
        self.assertEqual(2, len(writes))
        self.assertEqual("POST", writes[0][0])
        self.assertEqual("PATCH", writes[1][0])
        self.assertEqual(1, provider.comments[0].count(marker))
        self.assertFalse(any("merge" in path.lower() for _, path, _ in provider.calls))


class AttestationTest(unittest.TestCase):
    HEAD = "a" * 40
    MERGE = "b" * 40
    BLOB = "c" * 40
    PATH = ".github/workflows/celestrak-live.yml"
    JOB = "Compare reviewed CelesTrak table parser with official CSV"
    ROOT = "repos/org/repo"

    def setUp(self):
        self.config = RepositoryConfig(
            provider="github", project="org/repo", api_base_url="https://github.example/api/v3",
            clone_url="https://github.example/org/repo.git", base_branch="main",
            allowed_path_globs=("src/**",), denied_path_globs=(".git/**",),
            max_changed_files=2, max_diff_bytes=1000, token="secret", service_account_id="bot-service",
        )
        repo = {"id": 42, "full_name": "org/repo"}
        association = {
            "number": 7, "head": {"sha": self.HEAD, "repo": repo},
            "base": {"ref": "main", "repo": repo},
        }
        run = {
            "id": 123, "event": "pull_request", "head_sha": self.MERGE,
            "head_branch": "candidate", "path": self.PATH, "workflow_id": 9,
            "repository": repo, "head_repository": repo, "pull_requests": [association],
            "check_suite_id": 77, "status": "completed", "conclusion": "success",
        }
        check_url = f"https://github.example/api/v3/{self.ROOT}/check-runs/555"
        self.responses = {
            f"{self.ROOT}/pulls/7": {
                "number": 7, "state": "open", "head": {"sha": self.HEAD, "ref": "candidate", "repo": repo},
                "base": {"ref": "main", "repo": repo}, "merge_commit_sha": self.MERGE,
            },
            f"{self.ROOT}/actions/workflows/celestrak-live.yml": {
                "id": 9, "path": self.PATH, "state": "active",
            },
            f"{self.ROOT}/contents/{self.PATH}?ref=main": {"sha": self.BLOB},
            f"{self.ROOT}/contents/{self.PATH}?ref={self.MERGE}": {"sha": self.BLOB},
            f"{self.ROOT}/actions/workflows/9/runs?event=pull_request&branch=candidate&per_page=100":
                {"total_count": 1, "workflow_runs": [run]},
            f"{self.ROOT}/pulls?state=open&head=org:candidate&per_page=2": [
                {"number": 7, "head": {"sha": self.HEAD}, "base": {"ref": "main"}},
            ],
            f"{self.ROOT}/actions/runs/123/jobs?per_page=100": {
                "total_count": 1, "jobs": [{
                    "id": 555, "name": self.JOB, "run_id": 123, "head_sha": self.MERGE,
                    "check_run_url": check_url, "status": "completed", "conclusion": "success",
                }],
            },
            f"{self.ROOT}/check-runs/555": {
                "id": 555, "name": self.JOB, "head_sha": self.MERGE,
                "status": "completed", "conclusion": "success",
                "check_suite": {"id": 77}, "app": {"slug": "github-actions"},
            },
        }
        responses = self.responses

        class FakeAttestationProvider(GitHubProvider):
            def _request(self, method, path, *, payload=None):
                if method != "GET":
                    raise AssertionError("attestation must only read")
                return deepcopy(responses[path])

        self.provider = FakeAttestationProvider(self.config)

    def attest(self):
        return self.provider.read_validation_workflow(7, self.HEAD, self.PATH, self.JOB)

    def test_exact_pr_head_and_base_workflow_job_check_attest_success(self):
        result = self.attest()
        self.assertEqual("passed", result["status"])
        self.assertEqual(self.HEAD, result["head_sha"])
        self.assertEqual(123, result["workflow_run_id"])
        self.assertEqual(555, result["check_run_id"])
        self.assertEqual("https://github.example/org/repo/actions/runs/123", result["run_url"])
        self.assertEqual("pull_request", result["event"])
        self.assertEqual("success", result["conclusion"])

    def test_github_omits_pr_association_but_unique_current_head_still_attests(self):
        run = self.responses[f"{self.ROOT}/actions/workflows/9/runs?event=pull_request&branch=candidate&per_page=100"]["workflow_runs"][0]
        run["pull_requests"] = []
        self.assertEqual("passed", self.attest()["status"])
        self.responses[f"{self.ROOT}/pulls?state=open&head=org:candidate&per_page=2"].append(
            {"number": 8, "head": {"sha": self.HEAD}, "base": {"ref": "other"}}
        )
        self.assertEqual("ambiguous_pull_request_head", self.attest()["diagnostic"])

    def test_source_sha_run_uses_base_merge_workflow_blob(self):
        run = self.responses[f"{self.ROOT}/actions/workflows/9/runs?event=pull_request&branch=candidate&per_page=100"]["workflow_runs"][0]
        run["head_sha"] = self.HEAD
        run["pull_requests"] = []
        self.responses[f"{self.ROOT}/actions/runs/123/jobs?per_page=100"]["jobs"][0]["head_sha"] = self.HEAD
        self.responses[f"{self.ROOT}/check-runs/555"]["head_sha"] = self.HEAD
        # Candidate branch need not contain a workflow added on the trusted base.
        self.assertEqual("passed", self.attest()["status"])

    def test_pending_and_completed_failure_are_distinct(self):
        run = self.responses[f"{self.ROOT}/actions/workflows/9/runs?event=pull_request&branch=candidate&per_page=100"]["workflow_runs"][0]
        run["status"], run["conclusion"] = "in_progress", None
        self.assertIsNone(self.attest())
        run["status"], run["conclusion"] = "completed", "failure"
        job = self.responses[f"{self.ROOT}/actions/runs/123/jobs?per_page=100"]["jobs"][0]
        check = self.responses[f"{self.ROOT}/check-runs/555"]
        job["conclusion"] = check["conclusion"] = "failure"
        result = self.attest()
        self.assertEqual(("failed", "workflow_not_successful", 555), (
            result["status"], result["diagnostic"], result["check_run_id"],
        ))

    def test_spoofed_run_identity_never_attests(self):
        runs = self.responses[f"{self.ROOT}/actions/workflows/9/runs?event=pull_request&branch=candidate&per_page=100"]
        run = runs["workflow_runs"][0]
        for key, value in (
            ("event", "workflow_dispatch"), ("path", ".github/workflows/spoof.yml"),
            ("head_sha", "d" * 40), ("head_repository", {"id": 99, "full_name": "fork/repo"}),
        ):
            with self.subTest(key=key):
                saved = run[key]
                run[key] = value
                self.assertEqual("failed", self.attest()["status"])
                run[key] = saved
        runs["total_count"] = 2
        runs["workflow_runs"].append(deepcopy(run))
        self.assertEqual("ambiguous_workflow_runs", self.attest()["diagnostic"])

    def test_wrong_pr_head_and_modified_workflow_are_not_passes(self):
        pr = self.responses[f"{self.ROOT}/pulls/7"]
        pr["head"]["sha"] = "d" * 40
        self.assertEqual("pr_head_changed", self.attest()["diagnostic"])
        pr["head"]["sha"] = self.HEAD
        self.responses[f"{self.ROOT}/contents/{self.PATH}?ref={self.MERGE}"]["sha"] = "e" * 40
        self.assertEqual("workflow_differs_from_base", self.attest()["diagnostic"])

    def test_fork_missing_check_or_rejected_workflow_identity_fails_closed(self):
        pr = self.responses[f"{self.ROOT}/pulls/7"]
        pr["head"]["repo"] = {"id": 99, "full_name": "fork/repo"}
        self.assertEqual("wrong_pr_repository_or_base", self.attest()["diagnostic"])
        pr["head"]["repo"] = pr["base"]["repo"]
        self.responses[f"{self.ROOT}/check-runs/555"]["app"]["slug"] = "other"
        self.assertEqual("untrusted_check_run", self.attest()["diagnostic"])
        self.responses[f"{self.ROOT}/actions/workflows/celestrak-live.yml"]["path"] = ".github/workflows/other.yml"
        self.assertEqual("untrusted_workflow_identity", self.attest()["diagnostic"])

    def test_same_named_job_on_another_pr_cannot_attest(self):
        runs = self.responses[f"{self.ROOT}/actions/workflows/9/runs?event=pull_request&branch=candidate&per_page=100"]
        runs["workflow_runs"][0]["pull_requests"][0]["number"] = 8
        self.assertEqual("failed", self.attest()["status"])
        runs["workflow_runs"][0]["pull_requests"][0]["head"]["sha"] = "d" * 40
        self.assertEqual("untrusted_workflow_run", self.attest()["diagnostic"])
        runs["workflow_runs"][0]["head_sha"] = "d" * 40
        self.assertIsNone(self.attest())  # Stale unrelated SHA is not a current run.

    def test_transient_api_failure_is_not_pending_or_success(self):
        def unavailable(method, path, *, payload=None):
            raise GitProviderTransientError("provider transient status 503")

        self.provider._request = unavailable
        with self.assertRaises(GitProviderTransientError):
            self.attest()

    def test_invalid_catalog_path_and_gitlab_capability_unavailable(self):
        with self.assertRaisesRegex(GitProviderError, "workflow path"):
            self.provider.read_validation_workflow(7, self.HEAD, "../untrusted.yml", self.JOB)
        gitlab = GitLabProvider(replace(self.config, provider="gitlab"))
        with self.assertRaisesRegex(GitProviderError, "capability unavailable"):
            gitlab.read_validation_workflow(7, self.HEAD, self.PATH, self.JOB)


if __name__ == "__main__":
    unittest.main()
