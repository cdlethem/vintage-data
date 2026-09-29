"""`airflow-bots` command line: inspect spend, run the bot by hand, and run evals."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, evals, ledger


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="airflow-bots", description=__doc__)
    parser.add_argument("--config", help="bots YAML (default: $BOTS_CONFIG)")
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="today's spend and recent agent runs")
    status.add_argument("--days", type=int, default=1)

    sweep = sub.add_parser("sweep", help="run one heal sweep; prints the work it finds")
    sweep.add_argument("--work", action="store_true", help="also run the agent on each item")
    sweep.add_argument("--dry-run", action="store_true", help="print GitHub writes and pushes instead of doing them")

    heal = sub.add_parser("heal", help="run the healer on one failing task now")
    heal.add_argument("dag_id")
    heal.add_argument("task_id")
    heal.add_argument("--dry-run", action="store_true")

    job = sub.add_parser("job", help="run a scheduled job now")
    job.add_argument("name")
    job.add_argument("--dry-run", action="store_true")

    ev = sub.add_parser("eval", help="prompt evals").add_subparsers(dest="eval_command", required=True)
    ev_run = ev.add_parser("run", help="replay cases, grade them, store the scores")
    ev_run.add_argument("cases", nargs="*", help="case files (default: every *.yml in evals.cases)")
    ev_run.add_argument("--label", default="", help="note stored with the run, e.g. what you changed")
    ev_run.add_argument("--repeat", type=int, default=1, help="attempts per case (models are not deterministic)")
    ev_run.add_argument("--parallel", type=int, default=1)
    ev_run.add_argument("--prompt", type=Path, help="candidate prompt template to use instead of the configured one")
    ev_run.add_argument("--agent", help="agent name to use instead of the configured one")
    ev_capture = ev.add_parser("capture", help="turn a recorded run into a case file")
    ev_capture.add_argument("run_id")
    ev_capture.add_argument("--id", dest="case_id")
    ev.add_parser("report", help="recent eval runs and pass rates")
    ev_show = ev.add_parser("show", help="per-case results of one eval run")
    ev_show.add_argument("run_id")
    ev_compare = ev.add_parser("compare", help="per-case pass rates of two eval runs")
    ev_compare.add_argument("first")
    ev_compare.add_argument("second")

    args = parser.parse_args(argv)
    cfg = config.load(args.config)

    if args.command == "status":
        return _status(cfg, args.days)
    if args.command in ("sweep", "heal", "job"):
        from . import workflows
        env = workflows.Env.create(cfg, dry_run=args.dry_run)
        if args.command == "sweep":
            items = workflows.plan(env)
            print(json.dumps(items, indent=2))
            for item in items if args.work else []:
                print(json.dumps(workflows.work(env, item), indent=2, default=str))
        elif args.command == "heal":
            since = datetime.now(timezone.utc) - timedelta(hours=cfg.heal.lookback_hours)
            failures = [ti for ti in env.airflow.failures(since)
                        if (ti["dag_id"], ti["task_id"]) == (args.dag_id, args.task_id)]
            item = workflows.classify(cfg, env.airflow, args.dag_id, args.task_id, failures) if failures else None
            if item is None:
                print(f"{args.dag_id}.{args.task_id} is not failing now and not flaky; nothing to do.")
                return 1
            print(json.dumps(workflows.work(env, item), indent=2, default=str))
        else:
            print(json.dumps(workflows.run_job(env, args.name), indent=2, default=str))
        return 0
    return _eval(cfg, args)


def _status(cfg: config.Config, days: int) -> int:
    runs, spent = ledger.today(cfg.state_dir)
    limits = cfg.limits
    print(f"today: {runs} agent runs (limit {limits.daily_runs or 'none'}), "
          f"${spent:.2f} spent (limit {'$%.2f' % limits.daily_usd if limits.daily_usd else 'none'})")
    reason = ledger.over_limit(cfg.state_dir, limits)
    if reason:
        print(f"PAUSED: {reason}")
    since = datetime.now(timezone.utc) - timedelta(days=days)
    for row in ledger.entries(cfg.state_dir, since)[-50:]:
        cost = "-" if row.get("cost_usd") is None else f"${row['cost_usd']:.3f}"
        link = f"issue #{row['issue']}" if row.get("issue") else ""
        link += f" pr #{row['pull']}" if row.get("pull") else ""
        print(f"{row['at'][:16]}  {row['kind']:<10} {row['subject'][:40]:<40} "
              f"{row.get('action') or 'error':<6} {cost:>7}  {link}  {row.get('error') or ''}".rstrip())
    return 0


def _eval(cfg: config.Config, args) -> int:
    command = args.eval_command
    if command == "run":
        files = [Path(p) for p in args.cases] or sorted((cfg.eval_cases or Path(".")).glob("*.yml"))
        evals.run(cfg, files, label=args.label, repeat=args.repeat, parallel=args.parallel,
                  prompt=args.prompt, agent_name=args.agent)
    elif command == "capture":
        print(evals.capture(cfg, args.run_id, args.case_id))
    elif command == "report":
        print(f"{'run':<16} {'when':<20} {'prompt':<12} {'agent':<14} {'pass':>7} {'cost':>8}  label")
        for run_id, when, label, phash, agent_name, passed, cases, cost in evals.report(cfg):
            print(f"{run_id:<16} {when[:19]:<20} {phash:<12} {agent_name[:14]:<14} "
                  f"{passed:>3}/{cases:<3} ${cost or 0:>7.3f}  {label}")
    elif command == "show":
        for case_id, attempt, passed, action, score, checks, criteria, error in evals.results(cfg, args.run_id):
            print(f"{'PASS' if passed else 'FAIL'} {case_id}#{attempt} action={action} score={score}")
            for name, (ok, detail) in json.loads(checks).items():
                if not ok:
                    print(f"    check {name}: {detail}")
            for item in json.loads(criteria):
                if not item.get("met"):
                    print(f"    unmet: {item.get('criterion')} - {item.get('note')}")
            if error:
                print(f"    error: {error}")
    elif command == "compare":
        print(f"{'case':<40} {args.first:>16} {args.second:>16}")
        for case_id, first, second in evals.compare(cfg, args.first, args.second):
            print(f"{case_id:<40} {_rate(first):>16} {_rate(second):>16}")
    return 0


def _rate(value: float | None) -> str:
    return "-" if value is None else f"{value:.0%}"


if __name__ == "__main__":
    sys.exit(main())
