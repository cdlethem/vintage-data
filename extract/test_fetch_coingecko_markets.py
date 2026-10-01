import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_coingecko_markets.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "coingecko_markets.yml"
SPEC = importlib.util.spec_from_file_location("fetch_coingecko_markets", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


COIN = {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "image": "https://coin-images.example/1.png",
    "current_price": 84240,
    "market_cap": 1692454148684,
    "market_cap_rank": 1,
    "fully_diluted_valuation": 1692454148684,
    "total_volume": 36055881402,
    "high_24h": 85518,
    "low_24h": 82951,
    "price_change_24h": 823.8,
    "price_change_percentage_24h": 0.99024,
    "circulating_supply": 20090909.0,
    "total_supply": 20090909.0,
    "max_supply": 21000000.0,
    "ath": 126080,
    "ath_change_percentage": -33.18538,
    "ath_date": "2025-10-06T10:57:42.000Z",
    "atl": 67.81,
    "atl_change_percentage": 124131.00125,
    "atl_date": "2013-07-05T16:53:36.483Z",
    "roi": None,
    "last_updated": "2026-10-01T05:31:30.000Z",
}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def coin_with_id(coin_id):
    coin = copy.deepcopy(COIN)
    coin["id"] = coin_id
    return coin


def http_error(code, reason="error"):
    return urllib.error.HTTPError(
        MODULE.BASE_URL, code, reason, {}, io.BytesIO(b"{}")
    )


class FetchCoinGeckoMarketsTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(document),
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            records = list(MODULE.fetch_markets(**kwargs))
        return records, urlopen, sleep

    def test_emits_envelope_with_stable_coin_id(self):
        records, _, _ = self.fetch([COIN])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "coingecko_markets")
        self.assertEqual(record["id"], "bitcoin")
        self.assertEqual(record["symbol"], "btc")
        self.assertEqual(record["name"], "Bitcoin")
        self.assertEqual(record["vs_currency"], "usd")
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, _, _ = self.fetch([COIN, coin_with_id("ethereum")])

        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_maps_market_fields_and_drops_untracked_fields(self):
        records, _, _ = self.fetch([COIN], vs_currency="eur")
        record = records[0]

        self.assertEqual(record["price"], 84240)
        self.assertEqual(record["vs_currency"], "eur")
        self.assertEqual(record["market_cap"], 1692454148684)
        self.assertEqual(record["market_cap_rank"], 1)
        self.assertEqual(record["total_volume"], 36055881402)
        self.assertEqual(record["price_change_24h"], 823.8)
        self.assertEqual(record["price_change_percentage_24h"], 0.99024)
        self.assertEqual(record["circulating_supply"], 20090909.0)
        self.assertEqual(record["total_supply"], 20090909.0)
        self.assertEqual(record["max_supply"], 21000000.0)
        self.assertEqual(record["ath"], 126080)
        self.assertEqual(record["ath_date"], "2025-10-06T10:57:42.000Z")
        self.assertEqual(record["ath_change_percentage"], -33.18538)
        self.assertEqual(record["atl"], 67.81)
        self.assertEqual(record["atl_date"], "2013-07-05T16:53:36.483Z")
        self.assertEqual(record["atl_change_percentage"], 124131.00125)
        self.assertEqual(record["last_updated"], "2026-10-01T05:31:30.000Z")
        for dropped in ("image", "roi", "high_24h", "low_24h", "fully_diluted_valuation"):
            self.assertNotIn(dropped, record)

    def test_makes_one_bounded_request_with_expected_query_and_user_agent(self):
        records, urlopen, _ = self.fetch([COIN], per_page=50, timeout=17)

        self.assertEqual(len(records), 1)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(parsed.scheme + "://" + parsed.netloc + parsed.path, MODULE.BASE_URL)
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(params["vs_currency"], ["usd"])
        self.assertEqual(params["order"], ["market_cap_desc"])
        self.assertEqual(params["per_page"], ["50"])
        self.assertEqual(params["page"], ["1"])
        self.assertEqual(params["sparkline"], ["false"])
        self.assertEqual(params["price_change_percentage"], ["24h"])
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_invalid_arguments_fail_before_http(self):
        cases = (
            {"vs_currency": "USD"},
            {"vs_currency": "u"},
            {"vs_currency": ""},
            {"vs_currency": "us$"},
            {"vs_currency": True},
            {"per_page": 0},
            {"per_page": MODULE.MAX_PER_PAGE + 1},
            {"per_page": -1},
            {"per_page": True},
            {"timeout": 0},
            {"timeout": MODULE.MAX_TIMEOUT + 1},
            {"timeout": True},
            {"retries": -1},
            {"retries": MODULE.MAX_RETRIES + 1},
            {"retries": True},
            {"retry_delay": -1},
            {"retry_delay": MODULE.MAX_RETRY_DELAY + 1},
            {"retry_delay": float("inf")},
            {"retry_delay": True},
        )
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(ValueError):
                        list(MODULE.fetch_markets(**kwargs))
                urlopen.assert_not_called()

    def test_retries_transient_429_then_succeeds(self):
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[
                    http_error(429, "Too Many Requests"),
                    JsonResponse([COIN]),
                ],
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            records = list(MODULE.fetch_markets(retries=2, retry_delay=1.5))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "bitcoin")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(1.5)

    def test_exhausted_retries_raise_after_bounded_attempts(self):
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[
                    http_error(429, "Too Many Requests"),
                    http_error(429, "Too Many Requests"),
                    http_error(429, "Too Many Requests"),
                ],
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 429"):
                list(MODULE.fetch_markets(retries=2))
        self.assertEqual(urlopen.call_count, 3)

    def test_non_transient_http_error_fails_immediately(self):
        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=http_error(400, "Bad Request")
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 400"):
                list(MODULE.fetch_markets(retries=2))
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_rejects_malformed_documents_and_rows(self):
        malformed = (
            ({"rows": [COIN]}, "not a list"),
            ([{}], "stable coin id"),
            ([{**COIN, "id": ""}], "stable coin id"),
            ([{**COIN, "id": 123}], "stable coin id"),
            ([{**COIN, "current_price": "84240"}], "non-numeric price"),
            ([{**COIN, "current_price": True}], "non-numeric price"),
            ([{**COIN, "current_price": float("nan")}], "non-numeric price"),
            ([{**COIN, "market_cap": "large"}], "non-numeric"),
            ([{**COIN, "total_volume": "36"}], "non-numeric"),
            ("not a list", "not a list"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_null_optional_fields_are_preserved_as_null(self):
        coin = copy.deepcopy(COIN)
        for field in ("market_cap_rank", "ath", "atl", "total_volume", "max_supply"):
            coin[field] = None

        records, _, _ = self.fetch([coin])

        record = records[0]
        for field in ("market_cap_rank", "ath", "atl", "total_volume", "max_supply"):
            self.assertIsNone(record[field])

    def test_empty_response_is_a_valid_empty_run(self):
        records, urlopen, _ = self.fetch([])

        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_main_emits_valid_ndjson_and_run_summary(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse([COIN, coin_with_id("ethereum")]),
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            MODULE.main(["--per-page", "250"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["id"], "bitcoin")
        self.assertEqual(json.loads(lines[1])["id"], "ethereum")

        summary_lines = [
            line
            for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        self.assertEqual(len(summary_lines), 1)
        summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX) :])
        self.assertEqual(summary["rows"], 2)
        self.assertEqual(summary["vs_currency"], "usd")
        self.assertEqual(summary["first_id"], "bitcoin")
        self.assertEqual(summary["last_id"], "ethereum")

    def test_main_emits_summary_for_empty_run(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=JsonResponse([])
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            MODULE.main([])

        self.assertEqual(stdout.getvalue(), "")
        summary_line = [
            line
            for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ][0]
        summary = json.loads(summary_line[len(MODULE.SUMMARY_PREFIX) :])
        self.assertEqual(summary["rows"], 0)
        self.assertIsNone(summary["first_id"])

    def test_source_configuration_is_hourly_and_attributed(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "27 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn(
            'args: ["--per-page", "250", "--timeout", "30", "--retries", "2", "--retry-delay", "20"]',
            text,
        )
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("free tier", text)
        self.assertIn("10,000 calls/month", text)
        self.assertIn("one snapshot per hour", text)


if __name__ == "__main__":
    unittest.main()
