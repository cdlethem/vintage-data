#!/usr/bin/env python3
"""Offline behavioral tests for the CISA KEV catalog extractor."""
import contextlib
import importlib.util
import io
import json
import pathlib
import unittest
from unittest.mock import patch


SCRIPT = pathlib.Path(__file__).with_name("fetch_cisa_kev.py")
SPEC = importlib.util.spec_from_file_location("fetch_cisa_kev", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def entry(cve_id="CVE-2026-0001", **overrides):
    base = {
        "cveID": cve_id,
        "vendorProject": "Acme",
        "product": "Router 100",
        "vulnerabilityName": "Acme Router 100 Stack Overflow",
        "dateAdded": "2026-10-01",
        "shortDescription": "A remote attacker can overflow a stack buffer.",
        "requiredAction": "Apply updates per vendor instructions.",
        "dueDate": "2026-10-30",
        "knownRansomwareCampaignUse": "Known",
        "forensicTriage": "Yes",
        "notes": "https://nvd.nist.gov/vuln/detail/CVE-2026-0001",
        "cwes": ["CWE-787"],
    }
    base.update(overrides)
    return base


def document(*entries):
    return {"title": "Known Exploited Vulnerabilities", "vulnerabilities": list(entries)}


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class CisaKevTests(unittest.TestCase):
    def run_extractor(self, payload, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            patch.object(
                module.urllib.request,
                "urlopen",
                return_value=Response(payload),
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            result = module.main(list(args))
        return result, stdout.getvalue().splitlines(), stderr.getvalue().splitlines()

    def test_success_emits_one_record_per_entry_with_envelope(self):
        first, second = entry(), entry(cve_id="CVE-2026-0002")
        result, output, errors = self.run_extractor(json.dumps(document(first, second)).encode())
        self.assertEqual(result, 0)
        self.assertEqual(errors, [])
        self.assertEqual(len(output), 2)
        records = [json.loads(line) for line in output]
        self.assertEqual([r["id"] for r in records], ["CVE-2026-0001", "CVE-2026-0002"])
        for record, source_entry in zip(records, (first, second)):
            self.assertEqual(record["source"], module.SOURCE)
            self.assertEqual(record["fetched_at"], records[0]["fetched_at"])
            for key, value in source_entry.items():
                self.assertEqual(record[key], value, key)

    def test_single_fetched_at_stamped_once_per_run(self):
        result, output, _ = self.run_extractor(
            json.dumps(document(entry(), entry("CVE-2026-0002"))).encode()
        )
        self.assertEqual(result, 0)
        fetched = {json.loads(line)["fetched_at"] for line in output}
        self.assertEqual(len(fetched), 1)

    def test_missing_cve_id_fails_with_no_records(self):
        result, output, errors = self.run_extractor(
            json.dumps(document(entry(), {**entry("CVE-2026-0002"), "cveID": None})).encode()
        )
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("cveID", errors[0])

    def test_missing_vulnerabilities_key_fails(self):
        result, output, errors = self.run_extractor(json.dumps({"title": "KEV"}).encode())
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertIn("vulnerabilities", errors[0])

    def test_non_date_date_added_fails(self):
        result, output, errors = self.run_extractor(
            json.dumps(document(entry(dateAdded="yesterday"))).encode()
        )
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertIn("dateAdded", errors[0])

    def test_non_object_response_fails(self):
        result, output, errors = self.run_extractor(json.dumps([entry()]).encode())
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertEqual(len(errors), 1)

    def test_rejects_out_of_range_timeout(self):
        with self.assertRaises(ValueError):
            list(module.fetch_kev(timeout=0))
        with self.assertRaises(ValueError):
            list(module.fetch_kev(timeout=module.MAX_TIMEOUT + 1))


if __name__ == "__main__":
    unittest.main()
