import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from airflow_bots import config, github, ledger, workflows
from airflow_bots.agent import Decision, Option, Result

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

    def log(self, ti):
        return "ERROR boom"

    def dag_file(self, dag_id):
        return "dags/x.py"

    def link(self, ti):
        return "http://airflow/log"


class FakeWorkspace:
    def __init__(self, root):
        self.root = root

    def fetch(self, branch):
        return "base"

    @contextmanager
    def checkout(self, sha, name):
        yield self.root

    def changes(self, worktree, sha):
        return [], ""

    def files(self, sha):
        return {"dags/x.py"}


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


def test_a_failure_soon_after_closing_reopens_the_issue_instead_of_starting_over(cfg, monkeypatch):
    key = workflows.failure_key("x", "run")
    closed = issue(3, key, closed_hours_ago=3)
    before = FakeAirflow([ti("x", hours_ago=3.5)], {("x", "run"): [ti("x", hours_ago=3.5)]})
    within_grace = FakeAirflow([ti("x", hours_ago=2.5)], {("x", "run"): [ti("x", hours_ago=2.5)]})
    for af in (before, within_grace):
        gh = FakeGitHub(closed=[closed])
        assert run_plan(cfg, af, gh, monkeypatch) == [] and gh.writes == []

    after = FakeAirflow([ti("x", hours_ago=1)], {("x", "run"): [ti("x", hours_ago=1)]})
    gh = FakeGitHub(closed=[closed])
    assert run_plan(cfg, after, gh, monkeypatch) == []  # no new agent run, no new issue
    assert ("update_issue", (3,), {"state": "open"}) in gh.writes

    fixed = issue(4, key, status="review", closed_hours_ago=3)
    gh = FakeGitHub(closed=[fixed])
    run_plan(cfg, after, gh, monkeypatch)
    assert ("set_status", (4, "question", ["bots", "bots:review"]), {}) in gh.writes

    declined = dict(closed, state_reason="not_planned")
    gh = FakeGitHub(closed=[declined])
    assert run_plan(cfg, after, gh, monkeypatch) == [] and gh.writes == []


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


def decision(routine=False):
    return Decision(question="Should the fetcher accept a shorter last page?", why="It loosens a guard.",
                    routine=routine, options=[Option("Accept it", "keeps data flowing", "looser check", chosen=True),
                                              Option("Leave the check", "strict", "task stays broken")])


@pytest.mark.parametrize("routine, files, reasons", [
    (True, ["extract/fetch.py"], []),
    (False, ["extract/fetch.py"], ["judgement call"]),
    (True, ["extract/fetch.py", "orchestration/dags/x.py"], ["outside the auto-merge allow-list"]),
    (False, ["orchestration/dags/x.py"], ["judgement call", "outside the auto-merge allow-list"]),
])
def test_a_fix_merges_by_itself_only_when_routine_and_on_the_allow_list(cfg, routine, files, reasons):
    result = Result(action="fix", title="t", summary="s", decision=decision(routine))
    blockers = workflows.merge_blockers(cfg.heal.auto_merge, result, files)
    assert len(blockers) == len(reasons) and all(r in b for r, b in zip(reasons, blockers))
    disabled = config.AutoMerge(enabled=False, paths=("**",))
    assert "turned off" in workflows.merge_blockers(disabled, result, files)[0]


def test_a_pr_that_needs_review_says_why_what_to_decide_and_the_trade_offs(cfg):
    result = Result(action="fix", title="t", summary="PokéAPI fails.", details="evidence", decision=decision())
    body = workflows.pr_body(result, workflows.merge_blockers(cfg.heal.auto_merge, result, ["dags/x.py"]), 7)
    why, question, table = body.index("It loosens a guard."), body.index("Should the fetcher"), body.index("| Option |")
    assert body.index("Fixes #7") < why < question < table < body.index("## Details")
    assert "`dags/x.py`" in body
    assert "| ✅ this PR | Accept it | keeps data flowing | looser check |" in body
    assert "|  | Leave the check | strict | task stays broken |" in body
    routine = replace(result, decision=decision(routine=True))
    assert "Merges automatically" in workflows.pr_body(routine, [], 7)


def test_daily_limits_stop_new_runs(cfg):
    assert ledger.over_limit(cfg.state_dir, cfg.limits) is None
    ledger.record(cfg.state_dir, kind="heal", cost_usd=0.5)
    ledger.record(cfg.state_dir, kind="heal", cost_usd=None)
    assert "run limit" in ledger.over_limit(cfg.state_dir, cfg.limits)
    with pytest.raises(workflows.OverBudget):
        workflows.work(workflows.Env(cfg, FakeGitHub(), None), {"kind": "failure"})


def failing_agent(monkeypatch, script):
    agent_cfg = config.Agent(command=(sys.executable, "-c", script), timeout_minutes=1, env=("PATH",))
    monkeypatch.setattr(config.Config, "agent", lambda self, name: agent_cfg)


def heal_item(dag="x"):
    return {"kind": "failure", "dag_id": dag, "task_id": "run", "pattern": "persistent", "failed_count": 2,
            "latest_failure": ti(dag)}


def test_a_crashed_agent_is_retried_once_before_asking_a_person(cfg, monkeypatch, tmp_path):
    af = FakeAirflow(history={("x", "run"): [ti("x"), ti("x", hours_ago=2)]})
    monkeypatch.setattr(workflows.Env, "airflow", property(lambda self: af))
    failing_agent(monkeypatch, "import sys; sys.exit(3)")
    gh = FakeGitHub()
    env = workflows.Env(cfg, gh, FakeWorkspace(tmp_path))
    with pytest.raises(workflows.AgentFailed):
        workflows.heal_failure(env, heal_item())
    assert not any(name == "create_issue" for name, _, _ in gh.writes)
    with pytest.raises(workflows.AgentFailed):
        workflows.heal_failure(env, heal_item())
    created = [args for name, args, _ in gh.writes if name == "create_issue"]
    assert len(created) == 1 and github.keys(created[0][1]) == [workflows.failure_key("x", "run")]
    assert [row["ok"] for row in ledger.entries(cfg.state_dir)] == [False, False]


def test_a_failure_tracked_since_planning_is_not_worked_twice(cfg, monkeypatch, tmp_path):
    af = FakeAirflow(history={("x", "run"): [ti("x")]})
    monkeypatch.setattr(workflows.Env, "airflow", property(lambda self: af))
    gh = FakeGitHub(open_issues=[issue(4, workflows.failure_key("x", "run"), status="question")])
    env = workflows.Env(cfg, gh, FakeWorkspace(tmp_path))
    assert "skipped" in workflows.heal_failure(env, heal_item())
    assert ledger.entries(cfg.state_dir) == []


def test_answering_on_a_bot_issue_rewrites_it_to_the_current_understanding(cfg):
    key = workflows.failure_key("x", "run")
    old = issue(6, key, status="question")
    old["body"] = f"old summary\n\n**Status:** ❓ Needs a decision\n**Task:** `x` › `run`, failing\n\n{github.key_marker(key)}"
    gh = FakeGitHub(open_issues=[old])
    result = Result(action="wait", title="X feed is down upstream", summary="The upstream API returns 503.")
    workflows._publish(workflows.Env(cfg, gh, None), result, [], Path("."), cfg.heal.auto_merge, issue=old,
                       labels=["bots", "bots:question"])
    (_, (number,), fields), = [w for w in gh.writes if w[0] == "update_issue"]
    assert number == 6 and fields["title"] == "X feed is down upstream"
    assert fields["body"].startswith("The upstream API returns 503.")
    assert "**Task:** `x` › `run`, failing" in fields["body"] and github.keys(fields["body"]) == [key]


def test_host_paths_are_mapped_into_the_repository():
    files = {"orchestration/dags/extract_dags.py", "extract/scripts/fetch.py"}
    assert workflows.locate("/srv/deploy/orchestration/dags/extract_dags.py", files) == (
        "orchestration/dags/extract_dags.py", "/srv/deploy")
    assert workflows.locate("/opt/airflow/dags/other.py", files) == ("/opt/airflow/dags/other.py", None)
