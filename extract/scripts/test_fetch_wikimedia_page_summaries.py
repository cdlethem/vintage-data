import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import urllib.error

import duckdb
import yaml


SCRIPT = Path(__file__).with_name("fetch_wikimedia_page_summaries.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "wikimedia_page_summaries.yml"
REPO_ROOT = SCRIPT.parents[2]
TRANSFORM_DIR = REPO_ROOT / "transform"
SPEC = importlib.util.spec_from_file_location("fetch_wikimedia_page_summaries", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

FETCHED_AT = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, url=None):
        payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(payload)
        self._url = url

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def summary(
    *,
    language="en",
    project="wikipedia",
    title="Albert Einstein",
    page_id=736,
    revision="123456",
    timestamp="2026-09-16T10:20:30Z",
    extract="German-born theoretical physicist",
    description="German-born theoretical physicist",
    wikibase_item="Q937",
    images=True,
):
    domain = MODULE.PROJECT_DOMAINS[project]
    document = {
        "type": "standard",
        "title": title,
        "displaytitle": title,
        "namespace": {"id": 0},
        "wikibase_item": wikibase_item,
        "titles": {"canonical": title, "normalized": title, "display": title},
        "pageid": page_id,
        "lang": language,
        "dir": "ltr",
        "revision": revision,
        "timestamp": timestamp,
        "description": description,
        "content_urls": {
            "desktop": {"page": f"https://{language}.{domain}/wiki/{title.replace(' ', '_')}"},
            "mobile": {"page": f"https://{language}.m.{domain}/wiki/{title.replace(' ', '_')}"},
        },
        "extract": extract,
    }
    if images:
        document["thumbnail"] = {
            "source": "https://upload.wikimedia.org/example-thumb.jpg",
            "width": 320,
            "height": 400,
        }
        document["originalimage"] = {
            "source": "https://upload.wikimedia.org/example.jpg",
            "width": 1024,
            "height": 1280,
        }
    return document


def http_error(code, url="https://en.wikipedia.org/api/rest_v1/page/summary/Albert_Einstein"):
    return urllib.error.HTTPError(url, code, "error", {}, io.BytesIO(b"error"))


class FetchWikimediaPageSummariesTests(unittest.TestCase):
    def fetch(self, pages, responses, **changes):
        arguments = {
            "timeout": 7,
            "retries": 0,
            "retry_budget": 30,
            "now": lambda: FETCHED_AT,
        }
        arguments.update(changes)
        opener = mock.Mock(side_effect=responses)
        client = MODULE.WikimediaClient(
            timeout=arguments.pop("timeout"),
            retries=arguments.pop("retries"),
            retry_budget=arguments.pop("retry_budget"),
            opener=opener,
            clock=arguments.pop("clock", None),
            sleeper=arguments.pop("sleeper", None),
        )
        records = MODULE.fetch_page_summaries(pages, client=client, **arguments)
        return records, opener

    def test_normalizes_response_and_preserves_nullable_fields(self):
        request = MODULE.make_page_request("EN", "Wikipedia", "  Albert Einstein  ")
        records, opener = self.fetch(
            [["EN", "Wikipedia", "  Albert Einstein  "]],
            [JsonResponse(summary(), url=request.url)],
        )

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["fetched_at"], "2026-09-17T12:00:00+00:00")
        self.assertEqual(record["id"], request.identity)
        self.assertEqual(record["language"], "en")
        self.assertEqual(record["project"], "wikipedia")
        self.assertEqual(record["requested_title"], "Albert_Einstein")
        self.assertEqual(record["title"], "Albert Einstein")
        self.assertFalse(record["is_redirect"])
        self.assertEqual(record["page_id"], 736)
        self.assertEqual(record["revision_id"], "123456")
        self.assertEqual(record["revision_timestamp"], "2026-09-16T10:20:30+00:00")
        self.assertEqual(record["wikibase_item"], "Q937")
        self.assertEqual(record["thumbnail_width"], 320)
        call = opener.call_args
        self.assertEqual(call.kwargs, {"timeout": 7})
        self.assertEqual(call.args[0].headers["Accept"], "application/json")
        self.assertTrue(call.args[0].headers["User-agent"])

        nullable = summary(description=None, wikibase_item=None, images=False, extract="")
        nullable_request = MODULE.make_page_request("en", "wikipedia", "Albert_Einstein")
        records, _ = self.fetch(
            [["en", "wikipedia", "Albert_Einstein"]],
            [JsonResponse(nullable, url=nullable_request.url)],
        )
        record = records[0]
        self.assertIsNone(record["description"])
        self.assertIsNone(record["wikibase_item"])
        self.assertEqual(record["summary"], "")
        for field in (
            "thumbnail_source",
            "thumbnail_width",
            "thumbnail_height",
            "original_image_source",
            "original_image_width",
            "original_image_height",
        ):
            self.assertIsNone(record[field])

    def test_encodes_unicode_and_reserved_characters_as_one_path_segment(self):
        request = MODULE.make_page_request("fr", "wikipedia", "  Café / 100%?  ")
        self.assertEqual(request.requested_title, "Café_/_100%?")
        self.assertEqual(
            request.url,
            "https://fr.wikipedia.org/api/rest_v1/page/summary/Caf%C3%A9_%2F_100%25%3F",
        )
        self.assertNotIn("/Café", request.url)

    def test_identity_separates_languages_projects_and_normalizes_equivalent_titles(self):
        en_wikipedia = MODULE.make_page_request("en", "wikipedia", "Albert Einstein")
        en_equivalent = MODULE.make_page_request("EN", "Wikipedia", "Albert__Einstein")
        de_wikipedia = MODULE.make_page_request("de", "wikipedia", "Albert Einstein")
        en_wiktionary = MODULE.make_page_request("en", "wiktionary", "Albert Einstein")
        self.assertEqual(en_wikipedia.identity, en_equivalent.identity)
        self.assertEqual(
            len({en_wikipedia.identity, de_wikipedia.identity, en_wiktionary.identity}),
            3,
        )

    def test_input_bounds_and_allowlists_fail_before_http(self):
        invalid = (
            ([], "at least one"),
            ([['en.wikipedia.org', 'wikipedia', 'x']], "language"),
            ([['en', 'commons', 'x']], "project"),
            ([['en', 'wikipedia', '']], "title"),
            ([['en', 'wikipedia', 'x\nmalicious']], "control"),
            ([['en', 'wikipedia', 'x' * (MODULE.MAX_TITLE_BYTES + 1)]], "at most"),
            ([['en', 'wikipedia', 'same'], ['EN', 'Wikipedia', 'same']], "duplicate"),
            ([['en', 'wikipedia', str(i)] for i in range(MODULE.MAX_TITLES + 1)], "at most"),
            ([['en', 'wikipedia', '..']], "dot segment"),
        )
        for pages, message in invalid:
            with self.subTest(message=message):
                client = mock.Mock()
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.fetch_page_summaries(pages, client=client)
                client.get.assert_not_called()

        for changes, message in (
            ({"timeout": 0}, "timeout"),
            ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
            ({"retries": MODULE.MAX_RETRIES + 1}, "retries"),
            ({"retry_budget": 0}, "retry_budget"),
        ):
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.fetch_page_summaries(
                        [["en", "wikipedia", "x"]], client=mock.Mock(), **changes
                    )

    def test_rejects_untrusted_response_hosts_and_http_redirects(self):
        page = MODULE.make_page_request("en", "wikipedia", "Albert_Einstein")
        bad_url = summary()
        bad_url["content_urls"]["desktop"]["page"] = "https://example.com/wiki/Albert_Einstein"
        with self.assertRaisesRegex(MODULE.WikimediaError, "untrusted"):
            self.fetch(
                [["en", "wikipedia", "Albert_Einstein"]],
                [JsonResponse(bad_url, url=page.url)],
            )

        redirected = JsonResponse(summary(), url="https://evil.example/summary")
        with self.assertRaisesRegex(MODULE.WikimediaError, "redirect"):
            self.fetch([["en", "wikipedia", "Albert_Einstein"]], [redirected])

        with self.assertRaisesRegex(MODULE.WikimediaError, "redirect"):
            self.fetch(
                [["en", "wikipedia", "Albert_Einstein"]],
                [http_error(302)],
            )


    def test_forged_request_host_is_rejected_before_http(self):
        trusted = MODULE.make_page_request("en", "wikipedia", "Albert_Einstein")
        forged = MODULE.PageRequest(
            language=trusted.language,
            project=trusted.project,
            requested_title=trusted.requested_title,
            host="evil.example",
            url="https://evil.example/summary",
            identity=trusted.identity,
        )
        opener = mock.Mock()
        client = MODULE.WikimediaClient(
            timeout=1, retries=0, retry_budget=1, opener=opener
        )
        with self.assertRaisesRegex(MODULE.WikimediaError, "untrusted"):
            client.get(forged)
        opener.assert_not_called()
    def test_error_responses_size_limit_and_malformed_documents_fail_closed(self):
        page = MODULE.make_page_request("en", "wikipedia", "Albert_Einstein")
        for response, message in (
            (http_error(404), "HTTP 404"),
            (JsonResponse(b"{", url=page.url), "malformed JSON"),
            (JsonResponse(b"x" * (MODULE.MAX_RESPONSE_BYTES + 1), url=page.url), "byte limit"),
            (JsonResponse([], url=page.url), "JSON object"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.WikimediaError, message):
                    self.fetch([["en", "wikipedia", "Albert_Einstein"]], [response])

    def test_timeout_retries_are_finite_and_redact_exception_details(self):
        times = iter([0.0, 0.0, 0.0, 1.0, 1.0, 3.0, 3.0])
        sleeps = []
        opener = mock.Mock(
            side_effect=[
                TimeoutError("secret-one"),
                TimeoutError("secret-two"),
                TimeoutError("secret-three"),
            ]
        )
        client = MODULE.WikimediaClient(
            timeout=7,
            retries=2,
            retry_budget=30,
            opener=opener,
            clock=lambda: next(times),
            sleeper=sleeps.append,
        )
        with self.assertRaisesRegex(MODULE.WikimediaError, "after 3 attempts") as raised:
            MODULE.fetch_page_summaries(
                [["en", "wikipedia", "Albert_Einstein"]],
                client=client,
                now=lambda: FETCHED_AT,
            )
        self.assertNotIn("secret", str(raised.exception))
        self.assertEqual(opener.call_count, 3)
        self.assertEqual(sleeps, [1.0, 2.0])
        self.assertTrue(all(call.kwargs["timeout"] <= 7 for call in opener.call_args_list))

    def test_retry_budget_prevents_another_attempt(self):
        times = iter([0.0, 0.0, 0.5])
        opener = mock.Mock(side_effect=TimeoutError("private"))
        client = MODULE.WikimediaClient(
            timeout=1,
            retries=2,
            retry_budget=1,
            opener=opener,
            clock=lambda: next(times),
            sleeper=mock.Mock(),
        )
        with self.assertRaisesRegex(MODULE.RetryBudgetError, "remaining retry budget"):
            MODULE.fetch_page_summaries(
                [["en", "wikipedia", "Albert_Einstein"]], client=client
            )
        opener.assert_called_once()

    def test_repeated_and_updated_observations_keep_identity(self):
        first_page = MODULE.make_page_request("en", "wikipedia", "Albert_Einstein")
        first, _ = self.fetch(
            [["en", "wikipedia", "Albert_Einstein"]],
            [JsonResponse(summary(revision="100"), url=first_page.url)],
        )
        second, _ = self.fetch(
            [["en", "wikipedia", "Albert Einstein"]],
            [JsonResponse(summary(revision="101", extract="updated"), url=first_page.url)],
            now=lambda: datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertNotEqual(first[0]["fetched_at"], second[0]["fetched_at"])
        self.assertNotEqual(first[0]["revision_id"], second[0]["revision_id"])

    def test_main_buffers_batch_and_emits_valid_ndjson(self):
        pages = [
            ["en", "wikipedia", "Albert_Einstein"],
            ["de", "wikipedia", "Albert_Einstein"],
        ]
        requests = [MODULE.make_page_request(*page) for page in pages]
        responses = [
            JsonResponse(summary(extract="Café — theoretical physicist"), url=requests[0].url),
            JsonResponse(summary(language="de"), url=requests[1].url),
        ]
        stdout = io.StringIO()
        with (
            mock.patch.object(MODULE.urllib.request, "build_opener") as build_opener,
            contextlib.redirect_stdout(stdout),
        ):
            build_opener.return_value.open.side_effect = responses
            MODULE.main(
                [
                    "--page", "en", "wikipedia", "Albert_Einstein",
                    "--page", "de", "wikipedia", "Albert_Einstein",
                    "--retries", "0",
                ]
            )
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual([json.loads(line)["language"] for line in lines], ["en", "de"])
        self.assertEqual(json.loads(lines[0])["summary"], "Café — theoretical physicist")
        self.assertNotIn(": ", lines[0])

        stdout = io.StringIO()
        with (
            mock.patch.object(MODULE.urllib.request, "build_opener") as build_opener,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            build_opener.return_value.open.side_effect = [
                JsonResponse(summary(), url=requests[0].url),
                http_error(404),
            ]
            MODULE.main(
                [
                    "--page", "en", "wikipedia", "Albert_Einstein",
                    "--page", "de", "wikipedia", "Albert_Einstein",
                    "--retries", "0",
                ]
            )
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")

    def test_main_rejects_later_non_scalar_strings_without_partial_output(self):
        pages = [
            ["en", "wikipedia", "Albert_Einstein"],
            ["de", "wikipedia", "Albert_Einstein"],
        ]
        requests = [MODULE.make_page_request(*page) for page in pages]
        for field in ("extract", "description", "wikibase_item"):
            with self.subTest(field=field):
                invalid = summary(language="de")
                invalid[field] = "escaped-\ud800-surrogate"
                raw_output = io.BytesIO()
                stdout = io.TextIOWrapper(
                    raw_output, encoding="utf-8", errors="strict"
                )
                with (
                    mock.patch.object(
                        MODULE.urllib.request, "build_opener"
                    ) as build_opener,
                    contextlib.redirect_stdout(stdout),
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit) as raised,
                ):
                    build_opener.return_value.open.side_effect = [
                        JsonResponse(summary(), url=requests[0].url),
                        JsonResponse(invalid, url=requests[1].url),
                    ]
                    MODULE.main(
                        [
                            "--page", "en", "wikipedia", "Albert_Einstein",
                            "--page", "de", "wikipedia", "Albert_Einstein",
                            "--retries", "0",
                        ]
                    )
                stdout.flush()
                self.assertEqual(raised.exception.code, 1)
                self.assertEqual(raw_output.getvalue(), b"")

    def test_main_controls_utf8_serialization_failure_before_output(self):
        raw_output = io.BytesIO()
        stdout = io.TextIOWrapper(raw_output, encoding="utf-8", errors="strict")
        with (
            mock.patch.object(
                MODULE,
                "fetch_page_summaries",
                return_value=[{"summary": "valid"}, {"summary": "bad\udfff"}],
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(io.StringIO()) as stderr,
            self.assertRaises(SystemExit) as raised,
        ):
            MODULE.main(["--page", "en", "wikipedia", "Albert_Einstein"])
        stdout.flush()
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(raw_output.getvalue(), b"")
        self.assertIn("could not be serialized as UTF-8", stderr.getvalue())

    def test_source_configuration_is_disabled_bounded_and_documented(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["sink"], "local")
        args = MODULE.build_parser().parse_args(config["args"])
        self.assertLessEqual(len(args.page), MODULE.MAX_TITLES)
        self.assertLessEqual(args.timeout, MODULE.MAX_TIMEOUT)
        self.assertLessEqual(args.retries, MODULE.MAX_RETRIES)
        self.assertLessEqual(args.retry_budget, MODULE.MAX_RETRY_BUDGET)
        for page in args.page:
            MODULE.make_page_request(*page)
        text = " ".join(SOURCE_CONFIG.read_text(encoding="utf-8").split())
        for expected in (
            "External acceptance is pending",
            "never changes requested identity",
            "exact replay copies",
            "not a crawl",
            "CC BY-SA",
            "attribution",
        ):
            self.assertIn(expected, text)


class WikimediaWarehouseIntegrationTests(unittest.TestCase):
    maxDiff = None

    @staticmethod
    def _child_text(value):
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    @classmethod
    def _child_failure(cls, category, argv, stdout, stderr, redact):
        def sanitize(value):
            text = cls._child_text(value).replace(str(redact), "<tmp>")
            text = re.sub(
                r"(?i)(token|password|secret)=\S+", r"\1=<redacted>", text
            )
            return re.sub(
                r"(?i)\b(https?://)[^/\s:@]+(?::[^/\s@]*)?@",
                r"\1<redacted>@",
                text,
            )

        category_prefix = f"{category}: "
        command = sanitize(" ".join(argv[:4]))
        prefix = (category_prefix + command)[:4000]
        output = sanitize(cls._child_text(stdout) + "\n" + cls._child_text(stderr)).strip()
        available = max(0, 4000 - len(prefix) - (1 if output else 0))
        return prefix + ("\n" + output[-available:] if available else "")

    @classmethod
    def _run_child(cls, argv, *, cwd, env, redact):
        try:
            completed = subprocess.run(
                argv,
                cwd=cwd,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                cls._child_failure(
                    f"child timed out after {exc.timeout} seconds",
                    argv,
                    exc.stdout,
                    exc.stderr,
                    redact,
                )
            ) from None
        if completed.returncode:
            raise AssertionError(
                cls._child_failure(
                    f"child exited {completed.returncode}",
                    argv,
                    completed.stdout,
                    completed.stderr,
                    redact,
                )
            )
        return completed

    def test_run_child_reports_bounded_redacted_timeout(self):
        temporary = Path("/tmp/private-loader-run")
        timeout = subprocess.TimeoutExpired(
            ["loader"],
            120,
            output=b"partial stdout",
            stderr=(
                b"x" * 5000
                + f" {temporary}/db https://user:pass@example.test/data ".encode()
                + b"token=private-token secret=private-secret stderr-tail"
            ),
        )
        with (
            mock.patch.object(subprocess, "run", side_effect=timeout),
            self.assertRaises(AssertionError) as raised,
        ):
            self._run_child(
                ["loader", "https://user:pass@example.test/input"],
                cwd=temporary,
                env={},
                redact=temporary,
            )
        diagnostic = str(raised.exception)
        self.assertTrue(diagnostic.startswith("child timed out after 120 seconds:"))
        self.assertNotIn("child exited", diagnostic)
        self.assertLessEqual(len(diagnostic), 4000)
        self.assertIn("<tmp>", diagnostic)
        self.assertIn("https://<redacted>@example.test", diagnostic)
        self.assertIn("token=<redacted>", diagnostic)
        self.assertIn("secret=<redacted>", diagnostic)
        self.assertIn("stderr-tail", diagnostic)
        for sensitive in ("private-loader-run", "user:pass", "private-token", "private-secret"):
            self.assertNotIn(sensitive, diagnostic)

    def test_run_child_handles_timeout_without_stderr(self):
        timeout = subprocess.TimeoutExpired(
            ["loader"], 120, output="partial output", stderr=None
        )
        with (
            mock.patch.object(subprocess, "run", side_effect=timeout),
            self.assertRaises(AssertionError) as raised,
        ):
            self._run_child(
                ["loader"], cwd=Path("/tmp/run"), env={}, redact=Path("/tmp/run")
            )
        diagnostic = str(raised.exception)
        self.assertIn("child timed out after 120 seconds", diagnostic)
        self.assertIn("partial output", diagnostic)
        self.assertNotIn("None", diagnostic)

    def test_run_child_preserves_nonzero_status_and_normalizes_bytes(self):
        completed = subprocess.CompletedProcess(
            ["loader"],
            23,
            stdout=b"byte output",
            stderr=b"password=private https://name@example.test/path",
        )
        with (
            mock.patch.object(subprocess, "run", return_value=completed),
            self.assertRaises(AssertionError) as raised,
        ):
            self._run_child(
                ["loader"], cwd=Path("/tmp/run"), env={}, redact=Path("/tmp/run")
            )
        diagnostic = str(raised.exception)
        self.assertIn("child exited 23", diagnostic)
        self.assertIn("byte output", diagnostic)
        self.assertIn("password=<redacted>", diagnostic)
        self.assertIn("https://<redacted>@example.test/path", diagnostic)
        self.assertNotIn("password=private", diagnostic)
        self.assertNotIn("name@example.test", diagnostic)

    @staticmethod
    def _record(language, project, requested_title, document, observed_at):
        page = MODULE.make_page_request(language, project, requested_title)
        return MODULE.normalize_summary(document, page, observed_at)

    def test_real_loader_and_actual_models_enforce_refresh_and_replay_semantics(self):
        first_at = "2026-09-17T12:00:00+00:00"
        second_at = "2026-09-18T12:00:00+00:00"
        old = self._record(
            "en", "wikipedia", "Albert_Einstein", summary(revision="100"), first_at
        )
        german = self._record(
            "de",
            "wikipedia",
            "Albert_Einstein",
            summary(
                language="de",
                title="Albert Einstein",
                page_id=127,
                revision="200",
                extract="Physiker",
                description=None,
                wikibase_item=None,
                images=False,
            ),
            first_at,
        )
        updated = self._record(
            "en",
            "wikipedia",
            "Albert Einstein",
            summary(revision="101", extract="updated summary"),
            second_at,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_dir = root / "raw" / f"source={MODULE.SOURCE}" / "dt=2026-09-17"
            raw_dir.mkdir(parents=True)
            files = (
                ("batch_one.ndjson", [old, german], first_at),
                ("batch_two.ndjson", [old, updated], second_at),
            )
            for name, records, started_at in files:
                path = raw_dir / name
                path.write_text(
                    "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
                    encoding="utf-8",
                )
                path.with_name(path.name + ".meta.json").write_text(
                    json.dumps(
                        {
                            "schema_version": 2,
                            "status": "success",
                            "health": "healthy",
                            "completeness": "complete",
                            "records": len(records),
                            "started_at": started_at,
                            "script": SCRIPT.name,
                        }
                    ),
                    encoding="utf-8",
                )

            extract_db = root / "extract.duckdb"
            transform_db = root / "transform.duckdb"
            loader_config = root / "load.yml"
            loader_config.write_text(
                yaml.safe_dump(
                    {
                        "destination": "test",
                        "destinations": {
                            "test": {
                                "type": "duckdb",
                                "database": str(extract_db),
                                "threads": 1,
                                "memory_limit": "512MB",
                                "lock_timeout_s": 5,
                                "max_attempts": 1,
                            }
                        },
                        "paths": {
                            "source_root": str(root / "raw"),
                            "queue_dir": str(root / "queue"),
                        },
                        "schemas": {"raw": "raw", "meta": "_load"},
                        "defaults": {
                            "min_age_s": 0,
                            "sample_lines": 0,
                            "keep_payload": True,
                            "detect_temporal": True,
                            "column_types": {
                                "source": "string",
                                "id": "string",
                                "fetched_at": "timestamp",
                            },
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            clean_env = {
                "PATH": os.environ.get("PATH", ""),
                "HOME": str(root / "home"),
                "PYTHONIOENCODING": "utf-8",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            sitecustomize_dir = root / "sitecustomize"
            sitecustomize_dir.mkdir()
            (sitecustomize_dir / "sitecustomize.py").write_text(
                "from dbt.adapters.duckdb.connections import DuckDBConnectionManager\n"
                "_release = DuckDBConnectionManager.release\n"
                "def release_and_refresh(self):\n"
                "    connection = self.get_if_exists()\n"
                "    name = connection.name if connection is not None else None\n"
                "    _release(self)\n"
                "    self.set_connection_name(name)\n"
                "DuckDBConnectionManager.release = release_and_refresh\n"
                "from dbt.task import runnable\n"
                "class SynchronousPool:\n"
                "    def __init__(self, processes, *args, **kwargs):\n"
                "        self.max_threads = processes\n"
                "        self.max_microbatch_models = 1\n"
                "        self.closed = False\n"
                "    def close(self): self.closed = True\n"
                "    def join(self): pass\n"
                "    def terminate(self): self.closed = True\n"
                "    def is_closed(self): return self.closed\n"
                "runnable.DbtThreadPool = SynchronousPool\n",
                encoding="utf-8",
            )
            (root / "home").mkdir()
            loaded = self._run_child(
                [
                    sys.executable,
                    "-m",
                    "loader",
                    "--config",
                    str(loader_config),
                    "run-once",
                    "--sources",
                    MODULE.SOURCE,
                ],
                cwd=REPO_ROOT / "load",
                env=clean_env,
                redact=root,
            )
            load_result = json.loads(loaded.stdout)
            self.assertEqual(load_result["status"], "ok")
            self.assertEqual(load_result["files_loaded"], 2)
            self.assertEqual(load_result["rows_loaded"], 4)

            dbt_env = {
                **clean_env,
                "EXTRACT_WAREHOUSE": str(extract_db),
                "DBT_DEV_WAREHOUSE": str(transform_db),
                "EXTRACT_WAREHOUSE_THREADS": "1",
                "EXTRACT_WAREHOUSE_MEMORY_LIMIT": "512MB",
                "PYTHONPATH": str(sitecustomize_dir),
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
                "DBT_TARGET_PATH": str(root / "dbt-target"),
                "DBT_LOG_PATH": str(root / "dbt-logs"),
            }
            self._run_child(
                [
                    "dbt",
                    "build",
                    "--single-threaded",
                    "--no-static-parser",
                    "--profiles-dir",
                    ".",
                    "--select",
                    "+fct_wikimedia_page_summary_observation",
                    "--no-use-colors",
                ],
                cwd=TRANSFORM_DIR,
                env=dbt_env,
                redact=root,
            )

            connection = duckdb.connect(str(transform_db), read_only=True)
            try:
                rows = connection.execute(
                    """
                    select language, project, requested_title, revision_id, summary,
                           description, wikibase_item, thumbnail_source, observed_at
                    from transform_marts.fct_wikimedia_page_summary_observation
                    order by language, observed_at
                    """
                ).fetchall()
                lineage = connection.execute(
                    """
                    select source_batch_id
                    from transform_marts.fct_wikimedia_page_summary_observation
                    where language = 'en' and observed_at = timestamptz '2026-09-17 12:00:00+00:00'
                    """
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0:4], ("de", "wikipedia", "Albert_Einstein", "200"))
        self.assertIsNone(rows[0][5])
        self.assertIsNone(rows[0][6])
        self.assertIsNone(rows[0][7])
        self.assertEqual(rows[1][0:5], ("en", "wikipedia", "Albert_Einstein", "100", "German-born theoretical physicist"))
        self.assertEqual(rows[2][0:5], ("en", "wikipedia", "Albert_Einstein", "101", "updated summary"))
        self.assertEqual(lineage, ("batch_two",))


if __name__ == "__main__":
    unittest.main()
