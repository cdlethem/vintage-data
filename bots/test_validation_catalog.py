from __future__ import annotations

import unittest

import validation_catalog


class PublicSmokeCatalogTest(unittest.TestCase):
    def test_entries_are_fixed_confined_public_smokes(self):
        expected = {
            "smoke-arxiv-new": ("arxiv", "/work/extract/scripts/fetch_arxiv_new.py", "https://export.arxiv.org/api/query"),
            "smoke-musicbrainz": ("musicbrainz", "/work/extract/scripts/fetch_musicbrainz.py", "https://musicbrainz.org/ws/2/release/"),
            "smoke-digitraffic-rail": ("digitraffic_rail_live_trains", "/work/extract/scripts/fetch_digitraffic_rail.py", "https://rata.digitraffic.fi/api/v1/live-trains/station/HKI?minutes_before_departure=0&minutes_after_departure=60&minutes_before_arrival=0&minutes_after_arrival=60"),
            "smoke-open-library": ("open_library", "/work/extract/scripts/fetch_open_library.py", "https://openlibrary.org/recentchanges/add-book.json?limit=20"),
            "smoke-workday": ("workday", "/work/extract/scripts/fetch_job_boards.py", "https://2020companies.wd1.myworkdayjobs.com/wday/cxs/2020companies/external_careers/jobs"),
            "smoke-sensor-community": ("sensor_community", "/work/extract/scripts/fetch_sensor_community.py", "https://data.sensor.community/airrohr/v1/filter/country=DE"),
        }
        self.assertEqual(set(expected), set(validation_catalog.COMMANDS))
        for command_id, (record_type, candidate_script, source_url) in expected.items():
            entry = validation_catalog.COMMANDS[command_id]
            argv = entry["argv"]
            self.assertEqual("public_source_smoke", entry["recipe"])
            self.assertEqual("public-network-readonly", entry["capability"])
            self.assertEqual("public-egress-proxy-v1", entry["network_profile"])
            self.assertEqual(source_url, entry["source_url"])
            self.assertEqual(200, entry["expected_status"])
            self.assertEqual([], entry["credential_env"])
            self.assertLessEqual(entry["timeout_seconds"], 300)
            self.assertEqual("/usr/bin/bwrap", argv[0])
            self.assertTrue({"--unshare-all", "--new-session", "--die-with-parent", "--clearenv"} <= set(argv))
            self.assertNotIn("--share-net", argv)
            self.assertEqual(1, argv.count("{candidate_root}"))
            self.assertIn("/work", argv)
            self.assertIn(record_type, argv)
            self.assertIn(candidate_script, argv)
            self.assertEqual(["{candidate_root}"], [item for item in argv if "{candidate_root}" in item])

    def test_trusted_envelope_is_literal_bounded_python(self):
        compile(validation_catalog._EXTRACTOR_ENVELOPE, "validation-envelope", "exec")
        self.assertIn("extractor emitted duplicate ids", validation_catalog._EXTRACTOR_ENVELOPE)
        self.assertIn("extractor result exceeds record bound", validation_catalog._EXTRACTOR_ENVELOPE)
        self.assertIn('"type": record_type', validation_catalog._EXTRACTOR_ENVELOPE)

    def test_workday_entry_is_one_fixed_board_and_record(self):
        argv = validation_catalog.COMMANDS["smoke-workday"]["argv"]
        self.assertIn("2020companies|wd1|external_careers", argv)
        self.assertIn("--max-per-board", argv)
        self.assertEqual("1", argv[argv.index("--max-per-board") + 1])
        self.assertEqual("1", argv[argv.index("--workers") + 1])


if __name__ == "__main__":
    unittest.main()
