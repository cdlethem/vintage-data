"""Tests for algorithmic scheduling cadence detection.

    load/.venv/bin/python load/test_cadence.py

The cron classes also run under the Airflow venv, which has croniter — that
run cross-checks every managed source's interval against a real cron iterator:

    orchestration/.venv/bin/python load/test_cadence.py -k Cron

Three things are worth testing here and are tested: the cron arithmetic (a
wrong interval silently changes how often a public API is hit), the decision
law (including its floors, ceilings and streaks), and the probe SQL against a
real DuckDB file — the probe is the part that has to stay both correct and
cheap.
"""
from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from dataclasses import replace
from itertools import pairwise

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "orchestration" / "include"))

import yaml

import cadence_plan
from loader import cadence as C
from loader.config import (
    DagSettings,
    LoadConfig,
    ServiceSettings,
    SourceSettings,
    load_config,
)
# loader.destinations pulls in the duckdb driver, which only the load venv has:
# imported inside the one test class that needs a warehouse, so the pure cron
# and decision tests also run under the Airflow venv (which has croniter).

REPO_ROOT = HERE.parent


def policy(**over) -> C.CadencePolicy:
    base = C.CadencePolicy(plan_path=pathlib.Path("/tmp/plan.json"),
                           sources_dir=REPO_ROOT / "extract" / "sources")
    return replace(base, **over) if over else base


def source(name="demo", cron="*/5 * * * *", minutes=5, lo=5, hi=1440, ratio=0.0):
    return C.SourceCadence(name=name, declared_cron=cron, declared_minutes=minutes,
                           min_minutes=lo, max_minutes=hi, change_ratio=ratio,
                           volatile_keys=("fetched_at",))


def observation(rows=10, novel=1, signal="content"):
    return C.Observation(batch_id="b2", prev_batch_id="b1", rows=rows, distinct_rows=rows,
                         novel_rows=novel, novel_ratio=(novel / rows if rows else 0.0),
                         changed=novel > 0, signal=signal)


class CronArithmeticTest(unittest.TestCase):
    def test_only_expressible_intervals_are_rungs(self):
        for minutes in (5, 10, 12, 15, 20, 30, 60, 120, 240, 360, 480, 720, 1440):
            self.assertTrue(C.is_rung(minutes), minutes)
        for minutes in (0, 1, 4, 7, 45, 90, 100, 1500, 2880):
            self.assertFalse(C.is_rung(minutes), minutes)

    def test_synthesized_cron_has_the_interval_it_claims(self):
        for minutes in C.DEFAULT_LADDER:
            for name in ("hackernews", "queue_times", "some_very_long_source_name"):
                cron = C.cron_for(minutes, name)
                self.assertEqual(len(cron.split()), 5, cron)
                self.assertEqual(C.interval_minutes(cron), minutes, cron)

    def test_offsets_are_deterministic_and_spread(self):
        self.assertEqual(C.cron_for(15, "queue_times"), C.cron_for(15, "queue_times"))
        offsets = {C.cron_for(15, f"source_{i}").split()[0] for i in range(20)}
        self.assertGreater(len(offsets), 1)

    def test_reads_the_interval_out_of_hand_written_crons(self):
        cases = {
            "*/5 * * * *": 5,
            "2-57/5 * * * *": 5,
            "3-53/10 * * * *": 10,
            "11-59/15 * * * *": 15,
            "5,35 * * * *": 30,
            "45 * * * *": 60,
            "17 */4 * * *": 240,
            "17 0-23/6 * * *": 360,
            "30 6 * * *": 1440,
        }
        for cron, expected in cases.items():
            self.assertEqual(C.interval_minutes(cron), expected, cron)

    def test_non_uniform_or_day_scoped_schedules_have_no_single_cadence(self):
        for cron in ("0,7 * * * *", "0 9-17 * * *", "0 3 * * 1", "0 0 1 * *",
                     "* * * *", "bogus", "*/0 * * * *", "60 * * * *"):
            self.assertIsNone(C.interval_minutes(cron), cron)

    @unittest.skipUnless(
        __import__("importlib").util.find_spec("croniter"), "croniter not installed")
    def test_agrees_with_croniter_on_real_schedules(self):
        from datetime import datetime, timezone

        from croniter import croniter
        start = datetime(2026, 9, 4, 0, 0, tzinfo=timezone.utc)
        for path in sorted((REPO_ROOT / "extract" / "sources").glob("*.yml")):
            cron = (yaml.safe_load(path.read_text()) or {}).get("schedule")
            minutes = C.interval_minutes(cron) if cron else None
            if minutes is None:
                continue
            it = croniter(cron, start)
            fires = [it.get_next(datetime) for _ in range(5)]
            gaps = {int((b - a).total_seconds() // 60) for a, b in pairwise(fires)}
            self.assertEqual(gaps, {minutes}, f"{path.name}: {cron}")


class RungWindowTest(unittest.TestCase):
    def test_bounds_clamp_the_ladder(self):
        self.assertEqual(source(lo=10, hi=120).rungs(policy()), (10, 15, 30, 60, 120))

    def test_repo_floor_wins_over_a_reckless_yml(self):
        self.assertEqual(source(lo=1, hi=15).rungs(policy())[0], 5)

    def test_bounds_between_rungs_still_resolve(self):
        self.assertEqual(source(lo=7, hi=9).rungs(policy()), (5,))

    def test_snap_prefers_the_slower_rung_on_a_tie(self):
        self.assertEqual(C.snap(45, (30, 60)), 60)
        self.assertEqual(C.snap(7, (5, 10, 15)), 5)

    def test_returning_to_the_declared_interval_restores_the_declared_cron(self):
        src = source(cron="2-57/5 * * * *", minutes=5)
        self.assertEqual(src.plan_cron(5), "2-57/5 * * * *")
        self.assertNotEqual(src.plan_cron(10), "2-57/5 * * * *")
        self.assertEqual(C.interval_minutes(src.plan_cron(10)), 10)


class VolatilePatchTest(unittest.TestCase):
    def test_dotted_paths_become_a_nested_merge_patch(self):
        self.assertEqual(C.volatile_patch(["fetched_at", "page.updated_at"]),
                         {"fetched_at": None, "page": {"updated_at": None}})

    def test_siblings_share_a_branch(self):
        self.assertEqual(C.volatile_patch(["a.b", "a.c.d"]),
                         {"a": {"b": None, "c": {"d": None}}})

    def test_removing_a_whole_key_wins_over_a_deeper_path(self):
        self.assertEqual(C.volatile_patch(["page", "page.updated_at"]), {"page": None})

    def test_empty_and_degenerate_keys_are_ignored(self):
        self.assertEqual(C.volatile_patch([]), {})
        self.assertEqual(C.volatile_patch(["", "."]), {})


class DecisionTest(unittest.TestCase):
    def state(self, minutes=15, **over):
        base = C.CadenceState("demo", minutes, C.cron_for(minutes, "demo"))
        return replace(base, **over)

    def test_new_data_steps_faster(self):
        d = C.decide(policy(), source(), self.state(15), observation(rows=10, novel=3))
        self.assertEqual((d.decision, d.from_minutes, d.to_minutes), ("faster", 15, 10))
        self.assertEqual(C.interval_minutes(d.cron), 10)

    def test_no_new_data_steps_slower(self):
        d = C.decide(policy(), source(), self.state(15), observation(rows=10, novel=0))
        self.assertEqual((d.decision, d.to_minutes), ("slower", 30))

    def test_an_empty_run_is_not_change(self):
        obs = C.Observation("b2", "b1", 0, 0, 0, 0.0, False, "empty_batch")
        d = C.decide(policy(), source(), self.state(15), obs)
        self.assertEqual(d.decision, "slower")
        self.assertIn("no rows at all", d.reason)

    def test_floor_and_ceiling_hold_instead_of_stepping_off_the_ladder(self):
        src = source(lo=10, hi=60)
        fast = C.decide(policy(), src, self.state(10), observation(novel=5))
        self.assertEqual((fast.decision, fast.to_minutes), ("hold", 10))
        self.assertIn("floor", fast.reason)
        slow = C.decide(policy(), src, self.state(60), observation(novel=0))
        self.assertEqual((slow.decision, slow.to_minutes), ("hold", 60))
        self.assertIn("ceiling", slow.reason)

    def test_streak_thresholds_delay_a_step(self):
        p = policy(slow_down_after=2)
        first = C.decide(p, source(), self.state(15), observation(novel=0))
        self.assertEqual(first.decision, "hold")
        self.assertEqual(first.state.unchanged_streak, 1)
        second = C.decide(p, source(), first.state, observation(novel=0))
        self.assertEqual((second.decision, second.to_minutes), ("slower", 30))
        self.assertEqual(second.state.unchanged_streak, 0)

    def test_change_ratio_makes_a_trickle_count_as_unchanged(self):
        src = source(ratio=0.5)
        d = C.decide(policy(), src, self.state(15),
                     C.Observation("b2", "b1", 100, 100, 10, 0.1, False, "content"))
        self.assertEqual(d.decision, "slower")

    def test_a_step_resets_the_opposite_streak(self):
        state = self.state(15, unchanged_streak=3)
        d = C.decide(policy(), source(), state, observation(novel=1))
        self.assertEqual((d.state.changed_streak, d.state.unchanged_streak), (0, 0))

    def test_cold_start_holds_and_only_records_the_batch(self):
        obs = C.Observation("b1", None, 5, 5, 5, 1.0, True, "cold_start")
        d = C.decide(policy(), source(), self.state(15), obs)
        self.assertEqual((d.decision, d.to_minutes, d.state.last_batch_id),
                         ("hold", 15, "b1"))

    def test_a_hand_written_interval_off_the_ladder_snaps_before_stepping(self):
        state = C.CadenceState("demo", 45, "0 0-23/1 * * *")
        d = C.decide(policy(), source(), state, observation(novel=1))
        self.assertEqual((d.from_minutes, d.to_minutes), (60, 30))


class PolicyConfigTest(unittest.TestCase):
    def test_live_config_parses_and_ladder_is_expressible(self):
        p = load_config().cadence
        self.assertTrue(all(C.is_rung(m) for m in p.ladder_minutes))
        self.assertGreaterEqual(p.floor_minutes, C.FLOOR_MINUTES)

    def test_unexpressible_ladder_is_rejected(self):
        with self.assertRaises(ValueError):
            C.policy_from_config({"ladder_minutes": [5, 90]}, repo_root=REPO_ROOT)

    def test_unknown_setting_is_rejected(self):
        with self.assertRaises(ValueError):
            C.policy_from_config({"speed_up_aftr": 2}, repo_root=REPO_ROOT)

    def test_sub_floor_is_rejected(self):
        with self.assertRaises(ValueError):
            C.policy_from_config({"floor_minutes": 1}, repo_root=REPO_ROOT)


class SourceOptInTest(unittest.TestCase):
    def test_repo_sources_opt_in_explicitly_and_within_bounds(self):
        p = load_config().cadence
        managed = C.load_source_cadences(p)
        self.assertTrue(managed, "expected at least one cadence-managed source")
        for name, src in managed.items():
            self.assertGreaterEqual(src.min_minutes, C.FLOOR_MINUTES, name)
            self.assertLessEqual(src.min_minutes, src.max_minutes, name)
            self.assertIn(src.declared_minutes, src.rungs(p), name)

    def test_other_dag_factories_and_disabled_sources_are_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "a.yml").write_text(yaml.safe_dump(
                {"name": "a", "schedule": "*/5 * * * *", "dag_factory": "job_boards",
                 "cadence": {"auto": True}}))
            (root / "b.yml").write_text(yaml.safe_dump(
                {"name": "b", "schedule": "*/5 * * * *", "enabled": False,
                 "cadence": {"auto": True}}))
            (root / "c.yml").write_text(yaml.safe_dump(
                {"name": "c", "schedule": "0 3 * * 1", "cadence": {"auto": True}}))
            (root / "d.yml").write_text("name: d\nschedule: [broken\n")
            (root / "e.yml").write_text(yaml.safe_dump(
                {"name": "e", "schedule": "*/5 * * * *"}))
            with self.assertLogs("loader.cadence", level="WARNING"):
                managed = C.load_source_cadences(policy(sources_dir=root))
        self.assertEqual(managed, {})


class PlanFileTest(unittest.TestCase):
    def test_roundtrip_and_atomic_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = policy(plan_path=pathlib.Path(tmp) / "nested" / "plan.json")
            path = C.write_plan(p, {"demo": {"cron": "*/5 * * * *", "interval_minutes": 5}})
            self.assertEqual(C.read_plan(path)["sources"]["demo"]["interval_minutes"], 5)
            self.assertFalse(list(path.parent.glob("*.tmp")))

    def test_unreadable_plan_is_no_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = pathlib.Path(tmp) / "nope.json"
            self.assertEqual(C.read_plan(missing), {})
            broken = pathlib.Path(tmp) / "broken.json"
            broken.write_text("{not json")
            self.assertEqual(C.read_plan(broken), {})


class EffectiveScheduleTest(unittest.TestCase):
    """The Airflow seam: a bad plan must never change a schedule."""

    def cfg(self, **over):
        base = {"name": "demo", "schedule": "*/15 * * * *",
                "cadence": {"auto": True, "min_minutes": 10, "max_minutes": 120}}
        base.update(over)
        return base

    def test_plan_cron_is_used_when_it_is_in_bounds(self):
        cron, note = cadence_plan.effective_schedule(
            self.cfg(), {"demo": {"cron": "7 0-23/2 * * *", "decision": "slower",
                                  "reason": "no new rows"}})
        self.assertEqual(cron, "7 0-23/2 * * *")
        self.assertIn("cadence-managed", note)

    def test_out_of_bounds_plan_is_refused(self):
        with self.assertLogs(cadence_plan.log, level="WARNING"):
            cron, note = cadence_plan.effective_schedule(
                self.cfg(), {"demo": {"cron": "0 */8 * * *"}})
        self.assertEqual(cron, "*/15 * * * *")
        self.assertIn("out of bounds", note)

    def test_unusable_plan_cron_is_refused(self):
        with self.assertLogs(cadence_plan.log, level="WARNING"):
            cron, _ = cadence_plan.effective_schedule(
                self.cfg(), {"demo": {"cron": "0 9-17 * * *"}})
        self.assertEqual(cron, "*/15 * * * *")

    def test_sources_that_did_not_opt_in_keep_their_cron(self):
        cron, note = cadence_plan.effective_schedule(
            self.cfg(cadence=None), {"demo": {"cron": "*/5 * * * *"}})
        self.assertEqual(cron, "*/15 * * * *")
        self.assertIn("off", note)

    def test_missing_entry_keeps_the_declared_cron(self):
        cron, note = cadence_plan.effective_schedule(self.cfg(), {})
        self.assertEqual(cron, "*/15 * * * *")
        self.assertIn("no cadence plan entry", note)


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "duckdb driver not installed")
class ProbeAgainstDuckDBTest(unittest.TestCase):
    """The evaluator end to end against a real warehouse file."""

    def setUp(self):
        from loader.destinations import get_destination

        self.tmp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.tmp.name)
        self.sources_dir = root / "sources"
        self.sources_dir.mkdir()
        (self.sources_dir / "demo.yml").write_text(yaml.safe_dump(
            {"name": "demo", "script": "fetch_demo.py", "schedule": "*/15 * * * *",
             "cadence": {"auto": True, "min_minutes": 5, "max_minutes": 240}}))
        self.policy = policy(plan_path=root / "plan.json", sources_dir=self.sources_dir)
        self.config = LoadConfig(
            destination={"name": "test", "type": "duckdb",
                         "database": str(root / "test.duckdb")},
            source_root=root / "raw", queue_dir=root / "queue",
            raw_schema="raw", meta_schema="_load",
            service=ServiceSettings(), dag=DagSettings(), defaults=SourceSettings(),
            overrides={}, cadence=self.policy, path=root / "load.yml")
        self.destination = get_destination(self.config.destination, "raw", "_load")
        self.destination.connect()
        self.destination.ensure_schemas()
        self.destination.con.execute("""
            CREATE TABLE raw.demo (
                _batch_id VARCHAR, id VARCHAR, _payload JSON)""")
        self.clock = 1_800_000_000

    def tearDown(self):
        self.destination.close()
        self.tmp.cleanup()

    def batch(self, stamp, records, *, mtime=None):
        batch_id = f"demo_{stamp}"
        self.clock += 900
        for record in records:
            self.destination.con.execute(
                "INSERT INTO raw.demo VALUES (?, ?, ?::JSON)",
                [batch_id, record["id"], json.dumps(record)])
        self.destination.con.execute("""
            INSERT INTO _load."files" (path, source, table_name, batch_id, status,
                                       rows_loaded, file_mtime, attempts)
            VALUES (?, 'demo', 'demo', ?, 'loaded', ?, to_timestamp(?), 1)
        """, [f"/sink/{batch_id}.ndjson", batch_id, len(records),
              mtime if mtime is not None else self.clock])
        return batch_id

    def evaluate(self):
        result = {"errors": []}
        C.CadenceEvaluator(self.config, self.destination, self.policy).run(
            "load-1", result)
        return result["cadence"]

    def records(self, ids, *, fetched_at, score=1):
        return [{"source": "demo", "id": str(i), "fetched_at": fetched_at,
                 "score": score} for i in ids]

    def decisions(self):
        return self.destination.con.execute(
            "SELECT source, decision, from_minutes, to_minutes, novel_rows, signal, changed "
            "FROM _load.cadence_decisions ORDER BY decided_at, source").fetchall()

    def test_a_rerun_of_identical_records_is_not_change(self):
        # Same payload, new fetched_at: the naive _content_hash would differ on
        # every row. This is the case the whole module exists for.
        self.batch("20260904T120000Z", self.records(range(5), fetched_at="2026-09-04T12:00:00Z"))
        self.evaluate()                                    # cold start
        self.batch("20260904T121500Z", self.records(range(5), fetched_at="2026-09-04T12:15:00Z"))
        cadence = self.evaluate()
        self.assertEqual(cadence["slower"], 1)
        row = self.decisions()[-1]
        self.assertEqual((row[1], row[2], row[3], row[4], row[5], row[6]),
                         ("slower", 15, 30, 0, "content", False))

    def test_a_nested_volatile_key_stops_false_novelty(self):
        """The statuspage case: a per-poll timestamp inside an embedded object."""
        (self.sources_dir / "demo.yml").write_text(yaml.safe_dump(
            {"name": "demo", "script": "fetch_demo.py", "schedule": "*/15 * * * *",
             "cadence": {"auto": True, "min_minutes": 5, "max_minutes": 240,
                         "volatile_keys": ["fetched_at", "page.updated_at"]}}))

        def batch_records(stamp):
            return [{"source": "demo", "id": str(i), "fetched_at": stamp,
                     "page": {"id": "p1", "updated_at": stamp}, "status": "resolved"}
                    for i in range(4)]

        self.batch("20260904T120000Z", batch_records("t0"))
        self.evaluate()
        self.batch("20260904T121500Z", batch_records("t1"))
        cadence = self.evaluate()
        self.assertEqual(cadence["slower"], 1)
        self.assertEqual(self.decisions()[-1][4], 0)   # novel_rows

    def test_one_changed_value_on_a_stable_id_is_change(self):
        self.batch("20260904T120000Z", self.records(range(5), fetched_at="t0", score=1))
        self.evaluate()
        records = self.records(range(5), fetched_at="t1", score=1)
        records[2]["score"] = 99
        self.batch("20260904T121500Z", records)
        cadence = self.evaluate()
        self.assertEqual(cadence["faster"], 1)
        row = self.decisions()[-1]
        self.assertEqual((row[1], row[3], row[4]), ("faster", 10, 1))

    def test_an_empty_run_slows_without_touching_raw(self):
        self.batch("20260904T120000Z", self.records(range(3), fetched_at="t0"))
        self.evaluate()
        self.batch("20260904T121500Z", [])
        self.evaluate()
        self.assertEqual(self.decisions()[-1][5], "empty_batch")

    def test_no_new_run_means_no_decision(self):
        self.batch("20260904T120000Z", self.records(range(3), fetched_at="t0"))
        self.evaluate()
        second = self.evaluate()
        self.assertEqual(second["evaluated"], 0)
        self.assertIn("no new run", second["skipped"]["demo"])

    def test_a_pass_with_no_new_run_keeps_the_last_decision_in_the_plan(self):
        self.batch("20260904T120000Z", self.records(range(3), fetched_at="t0"))
        self.evaluate()
        self.batch("20260904T121500Z", self.records(range(3), fetched_at="t1"))
        self.evaluate()
        cadence = self.evaluate()          # nothing new to judge this time
        entry = C.read_plan(cadence["plan_path"])["sources"]["demo"]
        self.assertEqual(entry["decision"], "slower")
        self.assertEqual(entry["interval_minutes"], 30)
        self.assertIn("no new rows", entry["reason"])

    def test_oversized_batch_is_not_compared_record_by_record(self):
        self.policy = replace(self.policy, max_probe_rows=2)
        self.config = replace(self.config, cadence=self.policy)
        self.batch("20260904T120000Z", self.records(range(3), fetched_at="t0"))
        self.evaluate()
        self.batch("20260904T121500Z", self.records(range(3), fetched_at="t1"))
        self.evaluate()
        self.assertEqual(self.decisions()[-1][5], "rows_only")

    def test_state_and_plan_track_the_decision(self):
        self.batch("20260904T120000Z", self.records(range(3), fetched_at="t0"))
        self.evaluate()
        self.batch("20260904T121500Z", self.records(range(3), fetched_at="t1"))
        cadence = self.evaluate()
        state = self.destination.cadence_states()["demo"]
        self.assertEqual(state["interval_minutes"], 30)
        self.assertEqual(state["last_batch_id"], "demo_20260904T121500Z")
        plan = C.read_plan(cadence["plan_path"])["sources"]["demo"]
        self.assertEqual(plan["interval_minutes"], 30)
        self.assertEqual(C.interval_minutes(plan["cron"]), 30)
        # And the plan is what Airflow would act on.
        cfg = yaml.safe_load((self.sources_dir / "demo.yml").read_text())
        cron, note = cadence_plan.effective_schedule(cfg, {"demo": plan})
        self.assertEqual(cron, plan["cron"])
        self.assertIn("cadence-managed", note)

    def test_backfill_batches_never_drive_cadence(self):
        self.batch("20260904T120000Z", self.records(range(3), fetched_at="t0"))
        self.evaluate()
        self.destination.con.execute("""
            INSERT INTO _load."files" (path, source, table_name, batch_id, status,
                                       rows_loaded, file_mtime, attempts)
            VALUES ('/sink/demo_backfill_2004-04-28.ndjson', 'demo', 'demo',
                    'demo_backfill_2004-04-28', 'loaded', 900, to_timestamp(1900000000), 1)
        """)
        self.assertEqual([b["batch_id"] for b in self.destination.recent_batches("demo", 5)],
                         ["demo_20260904T120000Z"])
        self.assertEqual(self.evaluate()["evaluated"], 0)

    def test_ids_are_the_fallback_when_a_source_keeps_no_payload(self):
        self.destination.con.execute("CREATE TABLE raw.nopayload (_batch_id VARCHAR, id VARCHAR)")
        columns = self.destination.list_columns("raw", "nopayload")
        self.assertEqual(C.CadenceEvaluator._novelty_key(columns), "ids")
        self.destination.con.execute(
            "INSERT INTO raw.nopayload VALUES ('b1','1'),('b1','2'),('b2','2'),('b2','3')")
        probe = self.destination.probe_novelty("nopayload", "b2", "b1", key="ids",
                                               volatile_keys=("fetched_at",))
        self.assertEqual(probe, {"distinct_rows": 2, "novel_rows": 1})


if __name__ == "__main__":
    unittest.main()
