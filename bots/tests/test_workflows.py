from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from airflow_bots import config, github, ledger, workflows
from airflow_bots.agent import Result

NOW = datetime.now(timezone.utc)


def iso(delta_hours: float) -> str:
    return (NOW + timedelta(hours=delta_hours)).isoformat().replace("+00:00", "Z")


def ti(dag, task="run", state="failed", hours_ago=1.0):
    return {"dag_id": dag, "task_id": task, "state": state, "end_date": iso(-hours_ago),
            "dag_run_id": "r", "try_number": 1, "map_index": -1}


class FakeAirflow:
    def __init__(self, failures=(), history=None):
        self._failures = list(failures)
        self._history = history or {}

    def failures(self, since):
        return self._failures

    def history(self, dag_id, task_id, limit=20, since=None):
        rows = self._history.get((dag_id, task_id), [])
        return [row for row in rows if not since or row["end_date"] >= since.isoformat()[:19]][:limit]


class FakeGitHub:
    def __init__(self, open_issues=(), closed=(), muted=(), pulls=(), comments=(), threads=None, prs=None,
                 checks=None):
        self.open_issues, self.closed, self.muted = list(open_issues), list(closed), list(muted)
        self._pulls, self._comments, self.threads = list(pulls), list(comments), threads or {}
        self.prs, self.checks = prs or {}, checks or {}
        self.writes = []

    def issues(self, labels=github.LABEL, state="open", since=None):
        if github.MUTE_LABEL in labels:
            return self.muted
        return self.open_issues if state == "open" else self.closed

    def pulls(self, labels):
        return self._pulls

    def pull(self, number):
        return self.prs[number]

    def check_runs(self, sha):
        return self.checks.get(sha, [])

    def comments_since(self, since):
        return self._comments

    def comments(self, number):
        return self.threads.get(number, [])

    def issue(self, number):
        return next(i for i in self.open_issues + self.closed if i["number"] == number)

    def __getattr__(self, name):  # every write is recorded
        return lambda *args, **kwargs: self.writes.append((name, args, kwargs)) or {"number": 99}


def issue(number, key, status="waiting", created_hours_ago=5.0, closed_hours_ago=None):
    return {"number": number, "title": f"issue {number}", "body": f"text\n{github.key_marker(key)}",
            "labels": [{"name": github.LABEL}, {"name": github.STATUS_LABELS[status]}],
            "created_at": iso(-created_hours_ago), "state": "closed" if closed_hours_ago else "open",
            "closed_at": iso(-closed_hours_ago) if closed_hours_ago else None}


@pytest.fixture
def cfg(tmp_path: Path):
    path = tmp_path / "bots.yml"
    path.write_text(f"""
github: {{repo: acme/pipeline}}
airflow: {{url: http://airflow}}
state_dir: {tmp_path / 'state'}
agents: {{default: {{command: [agent, "{{prompt}}"]}}}}
limits: {{daily_runs: 2}}
heal:
  max_new_per_sweep: 2
  ignore: ["legacy_*"]
  auto_merge: {{enabled: true, paths: ["extract/*"]}}
""")
    return config.load(path)


def run_plan(cfg, af, gh, monkeypatch):
    monkeypatch.setattr(workflows.Env, "airflow", property(lambda self: af))
    return workflows.plan(workflows.Env(cfg, gh, None))


def test_plan_picks_persistent_untracked_failures_most_frequent_first(cfg, monkeypatch):
    key = workflows.failure_key
    af = FakeAirflow(
        failures=[ti("a"), ti("b"), ti("b"), ti("c"), ti("c"), ti("c"), ti("tracked"), ti("healed"),
                  ti("bots_heal", "work"), ti("legacy_x"), ti("muted")],
        history={(d, "run"): [ti(d)] for d in "abc"} | {
            ("tracked", "run"): [ti("tracked")], ("muted", "run"): [ti("muted")],
            ("healed", "run"): [ti("healed", state="success", hours_ago=0.2), ti("healed")]})
    gh = FakeGitHub(open_issues=[issue(1, key("tracked", "run"), status="review")],
                    muted=[issue(2, key("muted", "run"))])
    items = run_plan(cfg, af, gh, monkeypatch)
    assert [(i["dag_id"], i["failed_count"]) for i in items] == [("c", 3), ("b", 2)]


def test_recently_closed_issue_suppresses_the_old_failure_but_not_a_later_one(cfg, monkeypatch):
    key = workflows.failure_key("x", "run")
    closed = issue(3, key, closed_hours_ago=3)
    before = FakeAirflow([ti("x", hours_ago=3.5)], {("x", "run"): [ti("x", hours_ago=3.5)]})

    assert run_plan(cfg, before, FakeGitHub(closed=[closed]), monkeypatch) == []
    within_grace = FakeAirflow([ti("x", hours_ago=2.5)], {("x", "run"): [ti("x", hours_ago=2.5)]})
    assert run_plan(cfg, within_grace, FakeGitHub(closed=[closed]), monkeypatch) == []
    after = FakeAirflow([ti("x", hours_ago=1)], {("x", "run"): [ti("x", hours_ago=1)]})
    assert [i["dag_id"] for i in run_plan(cfg, after, FakeGitHub(closed=[closed]), monkeypatch)] == ["x"]


def test_intermittent_failures_are_investigated_and_closed_after_a_clean_window(cfg, monkeypatch):
    def alternating(dag, hours):  # a success between every failure: separate episodes
        return [ti(dag, state=state, hours_ago=h + offset) for h in hours for state, offset in
                (("success", 0), ("failed", 0.5))]
    burst = [ti("burst", state="success", hours_ago=0.1)] + [ti("burst", hours_ago=1 + i / 10) for i in range(5)]
    af = FakeAirflow(failures=[ti("flaky", hours_ago=h + 0.5) for h in (1, 3, 5)] + [ti("rare")]
                     + [ti("burst", hours_ago=1 + i / 10) for i in range(5)],
                     history={("flaky", "run"): alternating("flaky", (1, 3, 5)),
                              ("rare", "run"): [ti("rare", state="success", hours_ago=0.1)],
                              ("burst", "run"): burst})
    items = run_plan(cfg, af, FakeGitHub(), monkeypatch)
    assert [(i["dag_id"], i["pattern"]) for i in items] == [("flaky", "intermittent")]

    still_flaky = issue(8, workflows.flaky_key("flaky", "run"))
    clean = issue(9, workflows.flaky_key("clean", "run"))
    gh = FakeGitHub(open_issues=[still_flaky, clean])
    assert run_plan(cfg, af, gh, monkeypatch) == []
    closed = [args[0] for name, args, kwargs in gh.writes if name == "update_issue" and kwargs.get("state") == "closed"]
    assert closed == [9]


def test_recovered_waiting_issues_close_but_fix_issues_stay_open(cfg, monkeypatch):
    key = workflows.failure_key
    af = FakeAirflow(history={
        ("w", "run"): [ti("w", state="success", hours_ago=0.5)],
        ("q", "run"): [ti("q", hours_ago=0.5)],
        ("f", "run"): [ti("f", state="success", hours_ago=0.5)],
        ("old", "run"): [ti("old", state="success", hours_ago=9)]})
    gh = FakeGitHub(open_issues=[issue(1, key("w", "run")), issue(2, key("q", "run"), "question"),
                                 issue(3, key("f", "run"), "review"), issue(4, key("old", "run"))])
    run_plan(cfg, af, gh, monkeypatch)
    closed = [args[0] for name, args, kwargs in gh.writes if name == "update_issue" and kwargs.get("state") == "closed"]
    assert closed == [1]


def pr(number, sha="s1", mergeable=True, state=None):
    return {"number": number, "draft": False, "head": {"ref": f"bots/{number}-x", "sha": sha},
            "mergeable": mergeable, "mergeable_state": state or "clean"}


def check(status="completed", conclusion="success"):
    return {"name": "Tests", "status": status, "conclusion": conclusion}


@pytest.mark.parametrize("checks, mergeable, expected", [
    ([check()], True, "merge"),
    ([check(conclusion="failure")], True, "handoff"),
    ([check()], False, "handoff"),
    ([check(status="in_progress", conclusion=None)], True, None),
    ([], True, None),  # no CI at all: never merge blind
])
def test_automerge_only_merges_green_prs(cfg, monkeypatch, checks, mergeable, expected):
    gh = FakeGitHub(pulls=[{"number": 7}], prs={7: pr(7, mergeable=mergeable)}, checks={"s1": checks})
    run_plan(cfg, FakeAirflow(), gh, monkeypatch)
    names = [name for name, _, _ in gh.writes]
    if expected == "merge":
        assert names == ["merge"]
    elif expected == "handoff":
        assert "merge" not in names and ("remove_label", (7, github.AUTOMERGE_LABEL), {}) in gh.writes
    else:
        assert names == []


def test_bot_requests_need_a_trusted_author_and_no_prior_reply(cfg, monkeypatch):
    def comment(cid, number, association="COLLABORATOR", body="/bot please retry"):
        return {"id": cid, "body": body, "author_association": association, "user": {"login": "sam"},
                "issue_url": f"https://api/repos/acme/pipeline/issues/{number}"}
    gh = FakeGitHub(
        comments=[comment(1, 10), comment(2, 11, association="NONE"), comment(3, 12),
                  comment(4, 13, body="thanks bot")],
        threads={12: [{"body": "done <!-- bots:reply-to=3 -->"}]})
    items = run_plan(cfg, FakeAirflow(), gh, monkeypatch)
    assert items == [{"kind": "request", "number": 10, "comment_id": 1, "author": "sam", "request": "please retry"}]


def test_duplicate_result_links_the_failure_to_the_existing_issue(cfg):
    existing = issue(5, workflows.failure_key("a", "run"), status="waiting")
    gh = FakeGitHub(open_issues=[existing])
    env = workflows.Env(cfg, gh, None)
    result = Result(action="wait", title="t", summary="same outage", duplicate_of=5)
    facts = {"key": workflows.failure_key("b", "run"), "dag_id": "b", "task_id": "run"}
    outcome = workflows._publish(env, result, [], Path("."), cfg.heal.auto_merge, facts=facts)
    assert outcome == {"issue": 5, "duplicate": True}
    body = next(kwargs["body"] for name, args, kwargs in gh.writes if name == "update_issue")
    assert github.keys(body) == [workflows.failure_key("a", "run"), workflows.failure_key("b", "run")]
    assert not any(name == "create_issue" for name, _, _ in gh.writes)


def test_fix_without_changes_becomes_a_question(cfg):
    gh = FakeGitHub()
    env = workflows.Env(cfg, gh, None)
    result = Result(action="fix", title="t", summary="s")
    outcome = workflows._publish(env, result, [], Path("."), cfg.heal.auto_merge, facts={"key": "failure:a/run"})
    assert outcome["status"] == "question"
    assert not any(name == "create_pull" for name, _, _ in gh.writes)


@pytest.mark.parametrize("review, files, allowed", [
    (False, ["extract/fetch.py"], True),
    (True, ["extract/fetch.py"], False),
    (False, ["extract/fetch.py", "orchestration/dags/x.py"], False),
    (False, [], False),
])
def test_automerge_policy(cfg, review, files, allowed):
    result = Result(action="fix", title="t", summary="s", review=review)
    assert workflows.automerge_allowed(cfg.heal.auto_merge, result, files) is allowed


def test_daily_limits_stop_new_runs(cfg):
    assert ledger.over_limit(cfg.state_dir, cfg.limits) is None
    ledger.record(cfg.state_dir, kind="heal", cost_usd=0.5)
    ledger.record(cfg.state_dir, kind="heal", cost_usd=None)
    assert "run limit" in ledger.over_limit(cfg.state_dir, cfg.limits)
    with pytest.raises(workflows.OverBudget):
        workflows.work(workflows.Env(cfg, FakeGitHub(), None), {"kind": "failure"})
