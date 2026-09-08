from __future__ import annotations

import unittest

from airflow.providers.vintage.bot_dashboard.git_provider import (
    GitHubProvider,
    GitProviderError,
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


if __name__ == "__main__":
    unittest.main()
