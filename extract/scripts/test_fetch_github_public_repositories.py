import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import sys
import unittest
from unittest import mock
import urllib.error
import urllib.parse
import yaml


SCRIPT = Path(__file__).with_name("fetch_github_public_repositories.py")
REPO_ROOT = SCRIPT.parents[2]
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "github_public_repositories.yml"
RAW_SOURCES = REPO_ROOT / "transform" / "models" / "base" / "_raw_sources.yml"
BASE_MODEL = REPO_ROOT / "transform" / "models" / "base" / "base_github_public_repositories.sql"
SPEC = importlib.util.spec_from_file_location("fetch_github_public_repositories", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

from load.loader.config import SourceSettings
from load.loader.schema import infer_columns, table_columns


FETCHED_AT = "2026-09-17T12:34:56+00:00"


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, headers=None, url=None):
        super().__init__(json.dumps(document).encode("utf-8"))
        self.headers = headers or {}
        self.url = url

    def geturl(self):
        return self.url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def repository(repository_id=1, name="roadmap", **changes):
    value = {
        "id": repository_id,
        "node_id": f"R_{repository_id}",
        "name": name,
        "full_name": f"github/{name}",
        "private": False,
        "owner": {"login": "github", "id": 9919},
        "html_url": f"https://github.com/github/{name}",
        "description": "Public planning repository",
        "fork": False,
        "url": f"https://api.github.com/repos/github/{name}",
        "created_at": "2011-04-08T20:48:37Z",
        "updated_at": "2026-09-17T10:20:30Z",
        "pushed_at": "2026-09-16T09:08:07Z",
        "git_url": f"git://github.com/github/{name}.git",
        "ssh_url": f"git@github.com:github/{name}.git",
        "clone_url": f"https://github.com/github/{name}.git",
        "svn_url": f"https://github.com/github/{name}",
        "homepage": "https://github.com/",
        "stargazers_count": 2500,
        "watchers_count": 2500,
        "language": "Ruby",
        "has_issues": True,
        "has_projects": True,
        "has_wiki": True,
        "has_pages": False,
        "has_discussions": True,
        "forks_count": 320,
        "archived": False,
        "disabled": False,
        "open_issues_count": 14,
        "visibility": "public",
        "topics": ["github", "planning"],
        "custom_wire_field": {"retained": True},
    }
    value.update(changes)
    return value


def page_url(page, page_size=100, organization="github"):
    return (
        f"https://api.github.com/orgs/{organization}/repos?"
        + urllib.parse.urlencode({"type": "public", "per_page": page_size, "page": page})
    )


def link_header(*, next_page=None, last_page=None, page_size=100, organization="github"):
    links = []
    if next_page is not None:
        links.append(f'<{page_url(next_page, page_size, organization)}>; rel="next"')
    if last_page is not None:
        links.append(f'<{page_url(last_page, page_size, organization)}>; rel="last"')
    return ", ".join(links)


class FetchGitHubPublicRepositoriesTests(unittest.TestCase):
    def client(self, responses, *, retries=0, sleeper=None, wall_clock=None):
        opener = mock.Mock(side_effect=responses)
        client = MODULE.HttpClient(
            timeout=7,
            retries=retries,
            opener=opener,
            sleeper=sleeper or (lambda _: None),
            wall_clock=wall_clock or (lambda: 1_800_000_000),
        )
        return client, opener

    def fetch(self, responses, **changes):
        client, opener = self.client(responses)
        arguments = {
            "organization": "github",
            "page_size": 100,
            "max_pages": 20,
            "client": client,
            "fetched_at": FETCHED_AT,
        }
        arguments.update(changes)
        inventory = MODULE.fetch_github_public_repositories(**arguments)
        return inventory, opener

    def response(self, document, *, page=1, headers=None, page_size=100):
        return JsonResponse(
            document,
            headers=headers,
            url=page_url(page, page_size),
        )

    def test_normalizes_full_envelope_fields_types_and_preserves_wire_object(self):
        wire = repository()
        inventory, opener = self.fetch([self.response([wire])])

        self.assertTrue(inventory.complete)
        self.assertEqual(inventory.pages_fetched, 1)
        self.assertEqual(len(inventory.records), 1)
        record = inventory.records[0]
        required_types = {
            "source": str,
            "id": str,
            "fetched_at": str,
            "repository_id": int,
            "node_id": str,
            "organization": str,
            "name": str,
            "full_name": str,
            "owner_login": str,
            "private": bool,
            "visibility": str,
            "fork": bool,
            "archived": bool,
            "disabled": bool,
            "language": str,
            "description": str,
            "created_at": str,
            "updated_at": str,
            "pushed_at": str,
            "has_issues": bool,
            "has_projects": bool,
            "has_wiki": bool,
            "has_pages": bool,
            "has_discussions": bool,
            "open_issues_count": int,
            "stargazers_count": int,
            "watchers_count": int,
            "forks_count": int,
            "html_url": str,
            "api_url": str,
            "clone_url": str,
            "git_url": str,
            "ssh_url": str,
            "svn_url": str,
            "homepage": str,
            "raw_repository": dict,
        }
        self.assertEqual(set(record), set(required_types))
        for field, expected_type in required_types.items():
            self.assertIs(type(record[field]), expected_type, field)
        self.assertEqual(record["source"], "github_public_repositories")
        self.assertEqual(record["id"], "1")
        self.assertEqual(record["organization"], "github")
        self.assertEqual(record["created_at"], "2011-04-08T20:48:37+00:00")
        self.assertEqual(record["raw_repository"], wire)
        self.assertEqual(
            record["raw_repository"]["custom_wire_field"], {"retained": True}
        )

        request = opener.call_args.args[0]
        self.assertEqual(request.full_url, page_url(1))
        self.assertEqual(opener.call_args.kwargs, {"timeout": 7.0})
        self.assertEqual(request.headers["Accept"], "application/vnd.github+json")
        self.assertEqual(request.headers["X-github-api-version"], "2022-11-28")
        self.assertTrue(request.headers["User-agent"])
        self.assertIsNone(request.headers.get("Authorization"))

    def test_stable_identity_is_independent_of_snapshot_metadata(self):
        first = MODULE.normalize_repository(repository(), "github", FETCHED_AT)
        changed = repository(
            name="renamed",
            full_name="github/renamed",
            stargazers_count=9999,
            updated_at="2026-09-18T00:00:00Z",
        )
        second = MODULE.normalize_repository(changed, "github", "2026-09-18T01:00:00Z")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["repository_id"], second["repository_id"])
        self.assertNotEqual(first["full_name"], second["full_name"])
        self.assertNotEqual(first["fetched_at"], second["fetched_at"])

    def test_tolerates_null_and_missing_optional_metadata(self):
        minimal = {
            "id": 42,
            "name": "minimal",
            "full_name": "github/minimal",
            "private": False,
            "language": None,
            "description": None,
            "created_at": None,
            "updated_at": None,
            "pushed_at": None,
            "homepage": None,
        }
        record = MODULE.normalize_repository(minimal, "github", FETCHED_AT)
        optional = set(record) - {
            "source",
            "id",
            "fetched_at",
            "repository_id",
            "organization",
            "name",
            "full_name",
            "raw_repository",
            "private",
            "visibility",
        }
        for field in optional:
            self.assertIsNone(record[field], field)
        self.assertEqual(record["visibility"], "public")
        self.assertEqual(record["raw_repository"], minimal)

    def test_actual_load_contract_retains_complete_payload_and_infers_promoted_types(self):
        record = MODULE.normalize_repository(repository(), "github", FETCHED_AT)
        settings = SourceSettings(name=MODULE.SOURCE, keep_payload=True)
        columns = table_columns(infer_columns([record], settings), settings)
        by_name = {column.name: column.type for column in columns}

        self.assertEqual(by_name["_payload"], "json")
        self.assertEqual(by_name["id"], "string")
        self.assertEqual(by_name["fetched_at"], "timestamp")
        self.assertEqual(by_name["repository_id"], "integer")
        self.assertEqual(by_name["private"], "boolean")
        self.assertEqual(by_name["raw_repository"], "json")
        self.assertEqual(record["raw_repository"]["topics"], ["github", "planning"])

    def test_follows_multiple_pages_and_stops_at_terminal_link(self):
        first_url = page_url(1)
        second_url = page_url(2)
        responses = [
            JsonResponse(
                [repository(1, "one")],
                headers={"Link": link_header(next_page=2, last_page=2)},
                url=first_url,
            ),
            JsonResponse([repository(2, "two")], headers={}, url=second_url),
        ]
        inventory, opener = self.fetch(responses)

        self.assertTrue(inventory.complete)
        self.assertEqual(inventory.pages_fetched, 2)
        self.assertEqual([record["id"] for record in inventory], ["1", "2"])
        self.assertEqual([call.args[0].full_url for call in opener.call_args_list], [first_url, second_url])

    def test_deduplicates_repository_identifiers_across_pages(self):
        responses = [
            self.response(
                [repository(1, "one")],
                headers={"Link": link_header(next_page=2)},
            ),
            self.response([repository(1, "renamed"), repository(2, "two")], page=2),
        ]
        inventory, _ = self.fetch(responses)
        self.assertEqual([record["id"] for record in inventory], ["1", "2"])
        self.assertEqual(inventory.records[0]["name"], "one")

    def test_rejects_malformed_response_documents_and_repository_fields(self):
        invalid = (
            ({"message": "not found"}, "must be a JSON list"),
            (["not an object"], "non-object"),
            ([repository(id="bad")], "invalid id"),
            ([repository(name=None)], "invalid name"),
            ([repository(private="false")], "invalid private"),
            ([repository(stargazers_count=-1)], "invalid stargazers_count"),
            ([repository(created_at="yesterday")], "invalid created_at"),
            ([repository(owner="github")], "invalid owner"),
        )
        for document, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.GitHubResponseError, message):
                    self.fetch([self.response(document)])

    def test_rejects_non_json_wire_response(self):
        response = io.BytesIO(b"{not-json")
        response.headers = {}
        response.geturl = lambda: page_url(1)
        client, _ = self.client([response])
        with self.assertRaisesRegex(MODULE.GitHubResponseError, "not JSON"):
            client.get_json(page_url(1))

    def test_rejects_malformed_and_unsafe_pagination_destinations(self):
        links = (
            ("not a link", "malformed"),
            ('<http://api.github.com/orgs/github/repos?page=2&per_page=100&type=public>; rel="next"', "leaves"),
            ('<https://evil.example/orgs/github/repos?page=2&per_page=100&type=public>; rel="next"', "leaves"),
            ('<https://api.github.com/orgs/other/repos?page=2&per_page=100&type=public>; rel="next"', "leaves"),
            ('<https://api.github.com/orgs/github/repos?page=2&per_page=100&type=all>; rel="next"', "changes"),
            ('<https://api.github.com/orgs/github/repos?page=2&per_page=100&type=public&token=x>; rel="next"', "unexpected"),
            (
                '<https://api.github.com/orgs/github/repos?page=2&per_page=100&type=public>; rel="next", '
                '<https://api.github.com/orgs/github/repos?page=3&per_page=100&type=public>; rel="next"',
                "multiple next",
            ),
            (
                '<https://api.github.com/orgs/github/repos?page=3&per_page=100&type=public>; rel="next"',
                "consecutive page",
            ),
            (
                '<https://api.github.com/orgs/github/repos?page=2&per_page=100>; rel="next"',
                "changes",
            ),
            (
                '<https://api.github.com:bad/orgs/github/repos?page=2&per_page=100&type=public>; rel="next"',
                "leaves",
            ),
        )
        for link, message in links:
            with self.subTest(link=link):
                response = self.response([repository()], headers={"Link": link})
                with self.assertRaisesRegex(MODULE.GitHubResponseError, message):
                    self.fetch([response])

    def test_secondary_rate_limit_403_is_retried(self):
        error = urllib.error.HTTPError(
            page_url(1),
            403,
            "forbidden",
            {"Retry-After": "1"},
            io.BytesIO(b'{"message":"You have exceeded a secondary rate limit"}'),
        )
        delays = []
        client, opener = self.client(
            [error, self.response([repository()])], retries=1, sleeper=delays.append
        )
        inventory = MODULE.fetch_github_public_repositories(
            "github", client=client, fetched_at=FETCHED_AT
        )
        self.assertTrue(inventory.complete)
        self.assertEqual(delays, [1.0])
        self.assertEqual(opener.call_count, 2)

    def test_nonfinite_retry_header_uses_bounded_backoff(self):
        error = urllib.error.HTTPError(
            page_url(1),
            429,
            "rate limited",
            {"Retry-After": "nan"},
            io.BytesIO(b'{"message":"rate limit exceeded"}'),
        )
        delays = []
        client, _ = self.client(
            [error, self.response([repository()])], retries=1, sleeper=delays.append
        )
        inventory = MODULE.fetch_github_public_repositories(
            "github", client=client, fetched_at=FETCHED_AT
        )
        self.assertTrue(inventory.complete)
        self.assertEqual(delays, [1.0])

    def test_rejects_redirected_response(self):
        redirected = JsonResponse(
            [repository()],
            url="https://api.github.com/orgs/other/repos?type=public&per_page=100&page=1",
        )
        with self.assertRaisesRegex(MODULE.GitHubResponseError, "redirected"):
            self.fetch([redirected])

    def test_rate_limit_retries_using_retry_after_then_succeeds(self):
        error = urllib.error.HTTPError(
            page_url(1),
            429,
            "rate limited",
            {"Retry-After": "2", "X-RateLimit-Remaining": "0"},
            io.BytesIO(b'{"message":"rate limit exceeded"}'),
        )
        delays = []
        success = self.response([repository()])
        client, opener = self.client([error, success], retries=1, sleeper=delays.append)

        inventory = MODULE.fetch_github_public_repositories(
            "github", client=client, fetched_at=FETCHED_AT
        )
        self.assertTrue(inventory.complete)
        self.assertEqual(delays, [2.0])
        self.assertEqual(opener.call_count, 2)

    def test_rate_limit_with_unbounded_wait_fails_without_sleeping(self):
        error = urllib.error.HTTPError(
            page_url(1),
            403,
            "rate limited",
            {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1800000120"},
            io.BytesIO(b'{"message":"API rate limit exceeded"}'),
        )
        delays = []
        client, _ = self.client([error], retries=1, sleeper=delays.append)
        with self.assertRaisesRegex(MODULE.GitHubRateLimitError, "bounded maximum"):
            client.get_json(page_url(1))
        self.assertEqual(delays, [])

    def test_retry_exhaustion_is_an_explicit_failure(self):
        client, opener = self.client(
            [urllib.error.URLError("offline"), TimeoutError("timed out")], retries=1
        )
        with self.assertRaisesRegex(MODULE.GitHubInventoryError, "after 2 attempts"):
            client.get_json(page_url(1))
        self.assertEqual(opener.call_count, 2)

    def test_nonretryable_http_error_includes_bounded_failure_context(self):
        error = urllib.error.HTTPError(
            page_url(1), 404, "missing", {}, io.BytesIO(b'{"message":"Not Found"}')
        )
        client, _ = self.client([error])
        with self.assertRaisesRegex(
            MODULE.GitHubInventoryError, r"status=404.*Not Found"
        ):
            client.get_json(page_url(1))

    def test_page_cap_returns_partial_inventory_with_explicit_next_url(self):
        response = self.response(
            [repository()], headers={"Link": link_header(next_page=2, last_page=2)}
        )
        inventory, _ = self.fetch([response], max_pages=1)
        self.assertFalse(inventory.complete)
        self.assertEqual(inventory.pages_fetched, 1)
        self.assertEqual(inventory.next_url, page_url(2))
        diagnostic = MODULE._diagnostic(inventory, "github", 1)
        self.assertEqual(diagnostic["status"], "partial")
        self.assertIn("max_pages=1", diagnostic["reason"])

    def test_later_page_failure_cannot_emit_partial_ndjson(self):
        first = self.response(
            [repository()], headers={"Link": link_header(next_page=2)}
        )
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[first, urllib.error.URLError("offline")],
        ):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = MODULE.main(["--org", "github", "--retries", "0"])
        self.assertEqual(result, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(json.loads(stderr.getvalue())["status"], "failed")
        self.assertIn("offline", stderr.getvalue())

    def test_fixture_driven_cli_emits_nonempty_valid_ndjson_and_clean_stdout(self):
        fixture = [repository(101, "fixture")]
        response = self.response(fixture)
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = MODULE.main(["--org", "github", "--max-pages", "1"])

        self.assertEqual(result, 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["source"], "github_public_repositories")
        self.assertEqual(record["id"], "101")
        self.assertNotIn("status", record)
        self.assertNotIn("diagnostic", stdout.getvalue().lower())
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic["status"], "complete")
        self.assertEqual(diagnostic["records"], 1)

    def test_cli_page_cap_keeps_diagnostic_out_of_ndjson_and_exits_nonzero(self):
        response = self.response(
            [repository()], headers={"Link": link_header(next_page=2)}
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = MODULE.main(["--org", "github", "--max-pages", "1"])
        self.assertEqual(result, 2)
        self.assertEqual(len(stdout.getvalue().splitlines()), 1)
        self.assertEqual(json.loads(stderr.getvalue())["status"], "partial")

    def test_invalid_request_bounds_fail_before_http(self):
        cases = (
            ({"organization": "not/an/org"}, "organization"),
            ({"max_pages": 0}, "max_pages"),
            ({"max_pages": 101}, "max_pages"),
            ({"page_size": 0}, "page_size"),
            ({"page_size": 101}, "page_size"),
            ({"timeout": 0}, "timeout"),
            ({"retries": 6}, "retries"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.fetch_github_public_repositories(**changes)
                urlopen.assert_not_called()

    def test_source_manifest_pins_extractor_organization_and_complete_schedule(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r"(?m)^name: github_public_repositories$")
        self.assertRegex(text, r"(?m)^script: fetch_github_public_repositories.py$")
        self.assertIn('"--org", "github"', text)
        self.assertIn('"--max-pages", "100"', text)
        self.assertIn('"--page-size", "100"', text)
        self.assertRegex(text, r'(?m)^schedule: "[^"]+".*daily')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIn("cannot silently", text)
        self.assertIn("Organization-only, non-atomic snapshot", text)
        self.assertIn("authorized live-source", text)
        self.assertNotIn('"--max-pages", "1"', text)
        config = yaml.safe_load(text)
        self.assertEqual(config["name"], "github_public_repositories")
        self.assertEqual(config["script"], "fetch_github_public_repositories.py")
        self.assertEqual(config["args"][:2], ["--org", "github"])
        self.assertEqual(config["args"][2:4], ["--max-pages", "100"])
        self.assertFalse(config["enabled"])
        self.assertEqual(config["sink"], "local")

    def test_raw_source_declaration_and_base_relation_follow_conventions(self):
        inventory = RAW_SOURCES.read_text(encoding="utf-8")
        model = BASE_MODEL.read_text(encoding="utf-8")
        self.assertIn("  - name: github_public_repositories\n", inventory)
        self.assertIn("- name: base_github_public_repositories\n", inventory)
        self.assertIn("Complete raw record payload retained", inventory)
        self.assertIn("source('raw', 'github_public_repositories')", model)
        self.assertTrue(model.startswith("-- generated by transform/scripts/sync_raw_sources.py"))
        expected_columns = (
            "_payload",
            "source",
            "id",
            "fetched_at",
            "repository_id",
            "organization",
            "visibility",
            "language",
            "created_at",
            "has_issues",
            "open_issues_count",
            "stargazers_count",
            "forks_count",
            "html_url",
            "raw_repository",
        )
        for column in expected_columns:
            self.assertIn(f'    "{column}"', model)
            self.assertRegex(
                inventory,
                rf"(?m)^    - name: {re.escape(column)}$",
            )


if __name__ == "__main__":
    unittest.main()
