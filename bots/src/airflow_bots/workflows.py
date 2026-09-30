"""What the bot does: heal failing tasks, answer `/bot` requests, run scheduled jobs.

Every workflow ends the same way: an agent works in a fresh checkout and
returns a ``Result``; ``_publish`` turns it into GitHub issues, pull requests,
labels and comments. GitHub holds all ticket state; nothing else is stored
except the run directory (for inspection and evals) and the spend ledger.
"""
from __future__ import annotations

import fnmatch
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import agent, ledger
from .agent import Result
from .airflow_api import Airflow
from .config import PACKAGE_PROMPTS, AutoMerge, Config
from .github import (AUTOMERGE_LABEL, LABEL, MUTE_LABEL, REPLY_RE, STATUS_LABELS, GitHub, key_marker, keys)
from .workspace import Workspace

# A merged fix needs time to deploy and for the task to run again. Failures that
# ended within this window after an issue closed are treated as the old problem.
CLOSE_GRACE = timedelta(hours=1)
# Only people who can push to the repository may direct the bot.
TRUSTED = {"OWNER", "MEMBER", "COLLABORATOR"}
PASSING = {"success", "skipped", "neutral"}


class OverBudget(Exception):
    pass


class AgentFailed(RuntimeError):
    """The agent crashed, timed out, or returned no usable result (already recorded and published)."""


@dataclass
class Env:
    cfg: Config
    github: GitHub
    workspace: Workspace
    dry_run: bool = False

    @classmethod
    def create(cls, cfg: Config, dry_run: bool = False) -> "Env":
        return cls(cfg, GitHub.from_config(cfg, dry_run), Workspace(cfg, dry_run), dry_run)

    @property
    def airflow(self) -> Airflow:
        return Airflow.from_config(self.cfg)


def now() -> datetime:
    return datetime.now(timezone.utc)


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def failure_key(dag_id: str, task_id: str) -> str:
    return f"failure:{dag_id}/{task_id}"


def flaky_key(dag_id: str, task_id: str) -> str:
    return f"flaky:{dag_id}/{task_id}"


def failure_streaks(history: list[dict]) -> int:
    """Separate runs of consecutive failures (one outage counts once)."""
    return sum(1 for i, ti in enumerate(history)
               if ti["state"] == "failed" and (i + 1 == len(history) or history[i + 1]["state"] != "failed"))


def slug(text: str, limit: int = 40) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:limit].strip("-") or "change"


def status_of(issue: dict) -> str | None:
    names = {label["name"] for label in issue.get("labels", [])}
    return next((status for status, label in STATUS_LABELS.items() if label in names), None)


def ignored(cfg: Config, dag_id: str, task_id: str) -> bool:
    if dag_id == "bots_heal" or dag_id.startswith("bots_job__"):
        return True  # the bot does not diagnose itself; its failures show in Airflow
    return any(fnmatch.fnmatchcase(dag_id, pattern) or fnmatch.fnmatchcase(f"{dag_id}.{task_id}", pattern)
               for pattern in cfg.heal.ignore)


def automerge_allowed(policy: AutoMerge, result: Result, files: list[str]) -> bool:
    return (policy.enabled and not result.review and bool(files)
            and all(any(fnmatch.fnmatchcase(path, pattern) for pattern in policy.paths) for path in files))


# --------------------------------------------------------------------------- sweep
def plan(env: Env) -> list[dict]:
    """One heal sweep: tidy existing tickets, then return new work for agents.

    Housekeeping (closing recovered issues, merging green auto-merge PRs) happens
    here directly. The returned items are handled by ``work`` one agent run each.
    """
    cfg, gh = env.cfg, env.github
    af = env.airflow
    lookback = timedelta(hours=cfg.heal.lookback_hours)
    open_issues = gh.issues(state="open")
    open_keys = {key for issue in open_issues for key in keys(issue.get("body"))}
    recently_closed: dict[str, dict] = {}  # key -> the most recently closed issue tracking it
    for issue in gh.issues(state="closed", since=(now() - lookback - CLOSE_GRACE).isoformat()):
        for key in keys(issue.get("body")):
            if key not in recently_closed or issue["closed_at"] > recently_closed[key]["closed_at"]:
                recently_closed[key] = issue
    muted = {key for issue in gh.issues(labels=f"{LABEL},{MUTE_LABEL}", state="all") for key in keys(issue.get("body"))}

    failing: dict[tuple[str, str], list[dict]] = {}
    for ti in af.failures(now() - lookback):
        if not ignored(cfg, ti["dag_id"], ti["task_id"]):
            failing.setdefault((ti["dag_id"], ti["task_id"]), []).append(ti)

    _close_recovered(env, af, open_issues, failing)
    _automerge(env)

    items: list[dict] = []
    for (dag_id, task_id), tis in sorted(failing.items(), key=lambda kv: -len(kv[1])):
        if len(items) >= cfg.heal.max_new_per_sweep:
            break
        both = {failure_key(dag_id, task_id), flaky_key(dag_id, task_id)}
        if both & (open_keys | muted):
            continue
        item = classify(cfg, af, dag_id, task_id, tis)
        if item is None:
            continue  # it recovered on its own
        key = failure_key(dag_id, task_id) if item["pattern"] == "persistent" else flaky_key(dag_id, task_id)
        previous = recently_closed.get(key)
        if previous:
            if _time(tis[0]["end_date"]) >= _time(previous["closed_at"]) + CLOSE_GRACE:
                _reopen(env, previous, tis[0], af)
            continue
        items.append(item)
    items += _requests(env)
    return items


def _reopen(env: Env, issue: dict, latest: dict, af: Airflow) -> None:
    """A task failed again soon after its issue closed: continue that issue instead of starting a new one.

    Issues a person closed as "not planned" stay closed.
    """
    if issue.get("state_reason") == "not_planned":
        return
    gh, number = env.github, issue["number"]
    when = _fmt(latest["end_date"])
    note = f"🔁 Failing again: `{latest['dag_id']}` › `{latest['task_id']}` failed at {when} ([log]({af.link(latest)}))."
    if status_of(issue) in ("fixing", "review"):
        note += (" The fix tracked here did not stop it. Comment `/bot <instructions>` to have me look again, "
                 "or close this issue as not planned.")
        status = "question"
    else:
        note += " Reopening; this closes again once the task is healthy."
        status = status_of(issue) or "waiting"
    gh.update_issue(number, state="open")
    gh.comment(number, note)
    gh.set_status(number, status, [label["name"] for label in issue.get("labels", [])])


def classify(cfg: Config, af: Airflow, dag_id: str, task_id: str, failures: list[dict]) -> dict | None:
    """A work item if the task is failing now or keeps failing on and off; None if it is fine."""
    history = af.history(dag_id, task_id)
    window = timedelta(hours=cfg.heal.lookback_hours)
    if history and history[0]["state"] == "failed":
        pattern = "persistent"
    elif (cfg.heal.flaky_streaks and len(failures) >= cfg.heal.flaky_streaks and failure_streaks(
            af.history(dag_id, task_id, limit=1000, since=now() - window)) >= cfg.heal.flaky_streaks):
        pattern = "intermittent"
    else:
        return None
    latest = {k: failures[0][k] for k in ("dag_id", "task_id", "dag_run_id", "try_number", "map_index", "end_date")}
    return {"kind": "failure", "dag_id": dag_id, "task_id": task_id, "pattern": pattern,
            "failed_count": len(failures), "latest_failure": latest}


def _close_recovered(env: Env, af: Airflow, open_issues: list[dict], failing: dict) -> None:
    """Close failure issues that need no code change once their tasks are healthy again.

    A persistently failing task is healthy once its latest run succeeded; an
    intermittently failing one once it has had no failures for the whole lookback window.
    """
    for issue in open_issues:
        tracked = [key for key in keys(issue.get("body")) if key.startswith(("failure:", "flaky:"))]
        if not tracked or status_of(issue) not in ("waiting", "question"):
            continue
        opened = _time(issue["created_at"])
        recovered = []
        for key in tracked:
            kind, _, rest = key.partition(":")
            dag_id, _, task_id = rest.partition("/")
            if kind == "flaky":
                if (dag_id, task_id) in failing:
                    break
                recovered.append(f"`{dag_id}` › `{task_id}` has not failed for {env.cfg.heal.lookback_hours} hours")
                continue
            latest = (af.history(dag_id, task_id, limit=1) or [None])[0]
            if not latest or latest["state"] != "success" or _time(latest["end_date"]) < opened:
                break
            recovered.append(f"`{dag_id}` › `{task_id}` succeeded at {_fmt(latest['end_date'])}")
        else:
            env.github.comment(issue["number"], "✅ Recovered: " + "; ".join(recovered) + ". Closing.")
            env.github.update_issue(issue["number"], state="closed", state_reason="completed")


def _automerge(env: Env) -> None:
    """Merge bot PRs labelled for auto-merge once their checks pass; hand failures to a human."""
    gh = env.github
    for item in gh.pulls(AUTOMERGE_LABEL):
        number = item["number"]
        pr = gh.pull(number)
        if pr["draft"] or not pr["head"]["ref"].startswith("bots/"):
            continue
        runs = gh.check_runs(pr["head"]["sha"])
        failed = [run["name"] for run in runs if run["status"] == "completed" and run["conclusion"] not in PASSING]
        problem = None
        if failed:
            problem = f"Checks failed ({', '.join(failed)})"
        elif pr.get("mergeable") is False:
            problem = "This PR has merge conflicts"
        if problem:
            gh.remove_label(number, AUTOMERGE_LABEL)
            gh.add_labels(number, [STATUS_LABELS["review"]])
            gh.comment(number, f"{problem}, so I won't merge it automatically. "
                               "Comment `/bot <what to do>` to have me try again, or take it from here.")
            continue
        if not runs or any(run["status"] != "completed" for run in runs):
            continue  # no CI result yet: auto-merge never skips checks
        if pr.get("mergeable_state") == "behind":
            gh.update_branch(number)
            continue
        gh.merge(number, pr["head"]["sha"])


def _requests(env: Env) -> list[dict]:
    """`/bot ...` comments from trusted people that have not been answered yet."""
    gh = env.github
    items = []
    for comment in gh.comments_since((now() - timedelta(days=1)).isoformat()):
        body = (comment.get("body") or "").strip()
        if not body.startswith("/bot") or comment.get("author_association") not in TRUSTED:
            continue
        number = int(comment["issue_url"].rsplit("/", 1)[1])
        if any(str(comment["id"]) in REPLY_RE.findall(reply.get("body") or "") for reply in gh.comments(number)):
            continue
        items.append({"kind": "request", "number": number, "comment_id": comment["id"],
                      "author": comment["user"]["login"], "request": body.removeprefix("/bot").strip()})
    return items


# --------------------------------------------------------------------------- work
def work(env: Env, item: dict) -> dict:
    reason = ledger.over_limit(env.cfg.state_dir, env.cfg.limits)
    if reason:
        raise OverBudget(reason)
    if item["kind"] == "failure":
        return heal_failure(env, item)
    if item["kind"] == "request":
        return answer_request(env, item)
    raise ValueError(f"unknown work item {item!r}")


def open_issue_list(issues: list[dict], pulls: list[dict]) -> str:
    """What the bot already tracks, so an agent can spot duplicates (open PRs are not merged yet)."""
    lines = [f"- issue #{issue['number']} {issue['title']}" for issue in issues[:50]]
    lines += [f"- pull request #{pr['number']} {pr['title']} (not merged yet)" for pr in pulls[:50]]
    return "\n".join(lines) or "(none)"


def instructions(cfg: Config) -> str:
    return cfg.instructions.read_text() if cfg.instructions else ""


def build_prompt(cfg: Config, template: Path, variables: dict) -> str:
    """Render a prompt template. Evals call this with recorded variables."""
    return agent.render(template.read_text(), **variables,
                        instructions=instructions(cfg), result_contract=agent.result_contract())


def _run_agent(env: Env, kind: str, agent_name: str, template: Path, variables: dict, sha: str, subject: str,
               publish) -> dict:
    """Run one agent in a fresh checkout, publish its result, record it in the ledger."""
    cfg = env.cfg
    run_id = f"{now():%Y%m%dT%H%M%S}-{slug(subject, 60)}"
    run_dir = cfg.state_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(json.dumps(
        {"kind": kind, "template": str(template), "variables": variables, "base_sha": sha, "agent": agent_name},
        indent=2))
    prompt = build_prompt(cfg, template, variables)
    run, outcome = None, {}
    try:
        with env.workspace.checkout(sha, run_id) as worktree:
            run = agent.run(cfg.agent(agent_name), prompt, worktree, run_dir)
            files, diff = env.workspace.changes(worktree, sha)
            (run_dir / "diff.patch").write_text(diff)
            outcome = publish(run, files, worktree)
    finally:
        if run is not None:  # spend is recorded even when publishing fails
            ledger.record(cfg.state_dir, **{
                "run_id": run_id, "kind": kind, "subject": subject, "ok": run.ok, "error": run.error,
                "action": run.result.action if run.result else None, "cost_usd": run.cost_usd,
                "tokens": run.tokens, "seconds": round(run.seconds), **outcome})
    if not run.ok:  # after publishing any fallback: a failed agent run shows as a failed Airflow task
        raise AgentFailed(f"{run.error} (run {run_id}, outcome {outcome})")
    return {"run_id": run_id, **outcome}


def heal_failure(env: Env, item: dict) -> dict:
    cfg, af = env.cfg, env.airflow
    dag_id, task_id, count = item["dag_id"], item["task_id"], item["failed_count"]
    hours = cfg.heal.lookback_hours
    history = af.history(dag_id, task_id)
    if item["pattern"] == "persistent":
        if not history or history[0]["state"] != "failed":
            return {"skipped": "recovered before the bot got to it"}
        streak = next((i for i, ti in enumerate(history) if ti["state"] != "failed"), len(history))
        last_success = next((ti["end_date"] for ti in history if ti["state"] == "success"), None)
        since = _fmt(history[streak - 1]["end_date"])
        plural = "s" if count != 1 else ""
        pattern = (f"It has failed on every run since {since} ({count} failed run{plural} in the last {hours} hours). "
                   f"Last success: {_fmt(last_success) if last_success else f'none in the last {len(history)} runs'}.")
        key, failing_line = failure_key(dag_id, task_id), f"failing since {since} ({count} failed run{plural} in {hours}h)"
    else:
        window = af.history(dag_id, task_id, limit=1000, since=now() - timedelta(hours=hours))
        streaks = failure_streaks(window)
        pattern = (f"It fails intermittently: {count} of its {len(window)} runs in the last {hours} hours failed, "
                   f"in {streaks} separate episodes, and its latest run succeeded. Find out why some runs fail.")
        key, failing_line = flaky_key(dag_id, task_id), f"fails intermittently ({count} of {len(window)} runs in {hours}h)"
    latest = item["latest_failure"]
    open_issues = env.github.issues(state="open")
    if key in {k for issue in open_issues for k in keys(issue.get("body"))}:
        return {"skipped": "already tracked by an open issue"}
    sha = env.workspace.fetch(cfg.base_branch)
    dag_file, deploy_root = locate(af.dag_file(dag_id), env.workspace.files(sha))
    log = af.log(latest)
    if deploy_root:  # show repository paths, so the agent edits its checkout and not the live deployment
        log = log.replace(deploy_root + "/", "")
    variables = {"dag_id": dag_id, "task_id": task_id, "dag_file": dag_file, "pattern": pattern,
                 "log": log, "open_issues": open_issue_list(open_issues, env.github.pulls(LABEL))}
    facts = {"key": key, "dag_id": dag_id, "task_id": task_id, "failing": failing_line, "log_url": af.link(latest)}
    subject = f"{dag_id}.{task_id}"

    def publish(run: agent.Run, files: list[str], worktree: Path) -> dict:
        if not run.ok:
            since = now() - timedelta(hours=hours)
            if not any(e.get("subject") == subject and not e.get("ok") for e in ledger.entries(cfg.state_dir, since)):
                return {"retry": "the next sweep tries again before involving a person"}
        result = run.result or Result(
            action="ask", title=f"{dag_id} › {task_id} is failing",
            summary=f"`{dag_id}` › `{task_id}` keeps failing and the bot could not finish a diagnosis in two "
                    f"attempts (last error: {run.error}). Someone needs to look at the log.",
            details=f"```text\n{variables['log'][-3000:]}\n```",
            question="Can you take a look at this failure?")
        return _publish(env, result, files, worktree, cfg.heal.auto_merge, facts=facts)

    return _run_agent(env, "heal", cfg.heal.agent, cfg.heal.prompt, variables, sha, subject, publish)


def locate(fileloc: str, repo_files: set[str]) -> tuple[str, str | None]:
    """Map a path on the Airflow host to (path in the repository, deployment root), if it is in the repo."""
    parts = Path(fileloc).parts
    for i in range(1, len(parts)):
        candidate = "/".join(parts[i:])
        if candidate in repo_files:
            return candidate, str(Path(*parts[:i]))
    return fileloc, None


def answer_request(env: Env, item: dict) -> dict:
    cfg, gh = env.cfg, env.github
    number = item["number"]
    issue = gh.issue(number)
    is_pr = "pull_request" in issue
    branch = None
    if is_pr:
        branch = gh.pull(number)["head"]["ref"]
        if not branch.startswith("bots/"):
            gh.comment(number, f"I only change pull requests I opened.\n\n<!-- bots:reply-to={item['comment_id']} -->")
            return {"skipped": "not a bot pull request"}
    sha = env.workspace.fetch(branch or cfg.base_branch)
    thread = [f"**{issue['user']['login']}** (opened):\n{_strip_markers(issue.get('body') or '')[:4000]}"]
    thread += [f"**{c['user']['login']}**:\n{_strip_markers(c.get('body') or '')[:3000]}"
               for c in gh.comments(number)[-20:]]
    variables = {
        "thread_kind": "pull request" if is_pr else "issue", "number": number, "title": issue["title"],
        "author": item["author"], "request": item["request"] or "(no text: continue with the work)",
        "thread": "\n\n".join(thread),
        "checkout": (f"You are on this pull request's branch `{branch}`. If you change files, the bot pushes "
                     "them to the same pull request." if is_pr else
                     "You are on the latest main branch. If you change files, the bot opens a pull request "
                     "that closes this issue."),
        "open_issues": open_issue_list(gh.issues(state="open"), gh.pulls(LABEL)),
    }

    def publish(run: agent.Run, files: list[str], worktree: Path) -> dict:
        reply = f"<!-- bots:reply-to={item['comment_id']} -->"
        if not run.ok:
            gh.comment(number, f"Sorry, I couldn't finish that ({run.error}). Comment `/bot` again to retry."
                               f"\n\n{reply}")
            return {"replied": True}
        result = run.result
        labels = [label["name"] for label in issue.get("labels", [])]
        if is_pr:
            outcome = {}
            if result.action == "fix" and files:
                pushed = env.workspace.push(worktree, branch, result.title)
                automerge = automerge_allowed(cfg.heal.auto_merge, result, gh.pull_files(number) or files)
                gh.set_status(number, None if automerge else "review",
                              [name for name in labels if name != AUTOMERGE_LABEL] + ([AUTOMERGE_LABEL] if automerge else []))
                outcome = {"pushed": pushed}
            gh.comment(number, _reply_text(result) + "\n\n" + reply)
            return outcome
        if LABEL not in labels:
            labels.append(LABEL)
        outcome = _publish(env, result, files, worktree, cfg.heal.auto_merge, issue=issue, labels=labels)
        gh.comment(number, _reply_text(result) + "\n\n" + reply)
        return outcome

    return _run_agent(env, "request", cfg.heal.agent, cfg.respond_prompt, variables, sha,
                      f"reply-{number}", publish)


def run_job(env: Env, name: str) -> dict:
    cfg = env.cfg
    reason = ledger.over_limit(cfg.state_dir, cfg.limits)
    if reason:
        raise OverBudget(reason)
    job = cfg.jobs[name]
    sha = env.workspace.fetch(cfg.base_branch)
    variables = {"job_prompt": job.prompt.read_text(),
                 "open_issues": open_issue_list(env.github.issues(state="open"), env.github.pulls(LABEL))}

    def publish(run: agent.Run, files: list[str], worktree: Path) -> dict:
        if not run.ok:
            return {}
        result = run.result
        if result.action == "fix" and files:
            return _open_pull(env, result, files, worktree, job.auto_merge, issue_number=None)
        if result.action == "ask" or result.duplicate_of:
            return _publish(env, result, [], worktree, job.auto_merge, facts={"key": f"job:{name}:{slug(result.title)}"})
        return {"outcome": result.action}

    return _run_agent(env, f"job:{name}", job.agent, PACKAGE_PROMPTS / "job.md", variables, sha, name, publish)


# --------------------------------------------------------------------------- publishing
def _publish(env: Env, result: Result, files: list[str], worktree: Path, policy: AutoMerge, *,
             facts: dict | None = None, issue: dict | None = None, labels: list[str] | None = None) -> dict:
    """Record a result on GitHub: a new or existing issue, plus a PR when there is a fix."""
    gh = env.github
    facts = facts or {}
    key = facts.get("key")
    if key and issue is None:
        target_number = result.duplicate_of or next(
            (i["number"] for i in gh.issues(state="open") if key in keys(i.get("body"))), None)
        target = gh.issue(target_number) if target_number else None
        if target and target.get("state") == "open" and LABEL in {l["name"] for l in target.get("labels", [])}:
            what = f"`{facts['dag_id']}` › `{facts['task_id']}` is failing for the same reason" \
                if "dag_id" in facts else "Related finding"
            gh.comment(target["number"], f"{what}:\n\n{result.summary}")
            if key not in keys(target.get("body")):
                gh.update_issue(target["number"], body=(target.get("body") or "") + "\n" + key_marker(key))
            return {"issue": target["number"], "duplicate": True}
    if result.action == "fix" and not files:
        result.details = ("_The bot reported a fix but did not change any files._\n\n" + result.details).strip()
        result.action, result.question = "ask", result.question or "What should be done here?"
    if result.action == "none" and issue is not None:
        return {"issue": issue["number"]}  # a plain answer; the reply comment carries it
    status = {"fix": "review", "wait": "waiting", "ask": "question", "none": "waiting"}[result.action]

    task = _task_line(facts) if "dag_id" in facts else None
    if issue is None:
        issue = gh.create_issue(result.title, issue_body(result, status, task, [key] if key else []),
                                [LABEL, STATUS_LABELS[status]])
        issue.setdefault("title", result.title)
    number = issue["number"]
    outcome: dict = {"issue": number}
    pr = None
    if result.action == "fix":
        outcome.update(_open_pull(env, result, files, worktree, policy, issue_number=number))
        pr = outcome["pull"]
        status = "fixing" if outcome["automerge"] else "review"
    body = issue.get("body") or ""
    issue_keys = keys(body) or ([key] if key else [])
    if issue_keys:  # a bot issue always shows the current state on top; the history lives in comments
        new_body = issue_body(result, status, task or _existing_task_line(body), issue_keys, pr)
        if new_body != body or result.title != issue.get("title"):
            gh.update_issue(number, title=result.title, body=new_body)
    gh.set_status(number, status, labels or [LABEL])
    outcome["status"] = status
    return outcome


def _open_pull(env: Env, result: Result, files: list[str], worktree: Path, policy: AutoMerge,
               issue_number: int | None) -> dict:
    automerge = automerge_allowed(policy, result, files)
    branch = f"bots/{issue_number or now().strftime('%Y%m%d%H%M')}-{slug(result.title)}"
    message = result.title + (f"\n\nFixes #{issue_number}" if issue_number else "")
    env.workspace.push(worktree, branch, message)
    body = "\n\n".join(part for part in [
        result.summary, f"Fixes #{issue_number}" if issue_number else "", result.details, "---",
        ("This will merge automatically once all checks pass. Remove the `bots:automerge` label to stop that."
         if automerge else "Please review before merging. Comment `/bot <request>` to have me change it."),
    ] if part)
    pr = env.github.create_pull(branch, env.cfg.base_branch, result.title, body,
                                [LABEL, AUTOMERGE_LABEL if automerge else STATUS_LABELS["review"]])
    return {"pull": pr["number"], "branch": branch, "automerge": automerge}


def status_line(status: str, pr: int | None, failure: bool) -> str:
    return {
        "fixing": f"🔧 Fix in #{pr}; merges automatically once checks pass.",
        "review": f"👀 Fix in #{pr}; waiting for your review." if pr else "👀 Waiting for your review.",
        "waiting": "⏳ Nothing to change in the code right now. This closes by itself once the task is healthy again."
                   if failure else "⏳ Nothing to do right now.",
        "question": "❓ Needs a decision from you (below).",
    }[status]


def issue_body(result: Result, status: str, task: str | None, issue_keys: list[str], pr: int | None = None) -> str:
    lines = [result.summary, "", f"**Status:** {status_line(status, pr, task is not None)}"]
    if task:
        lines.append(task)
    if result.action == "ask" and result.question:
        lines += ["", f"**Decision needed:** {result.question}",
                  "", "Reply with `/bot <your answer>` and I'll carry on."]
    if result.details:
        lines += ["", "<details><summary>Details</summary>", "", result.details, "", "</details>"]
    lines += ["", *(key_marker(key) for key in issue_keys)]
    return "\n".join(lines).rstrip()


def _task_line(facts: dict) -> str:
    return (f"**Task:** `{facts['dag_id']}` › `{facts['task_id']}`, {facts['failing']} "
            f"([latest failed log]({facts['log_url']}))")


def _existing_task_line(body: str) -> str | None:
    match = re.search(r"^\*\*Task:\*\*.*$", body, flags=re.M)
    return match.group(0) if match else None


def _reply_text(result: Result) -> str:
    text = result.summary
    if result.action == "ask" and result.question:
        text += f"\n\n**Question:** {result.question}"
    if result.details:
        text += f"\n\n<details><summary>Details</summary>\n\n{result.details}\n\n</details>"
    return text


def _strip_markers(text: str) -> str:
    return re.sub(r"<!-- bots:[^>]*-->", "", text).strip()


def _fmt(value: str | None) -> str:
    return value[:16].replace("T", " ") + " UTC" if value else "unknown"

