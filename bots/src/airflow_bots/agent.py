"""Run a coding-agent CLI on a prompt and read back its structured result.

The agent is any command-line program (omp, claude, codex, aider, a docker
wrapper...). It works inside ``workdir`` (a git worktree) and ends its final
message with one JSON object; see ``prompts/result.md`` for the contract.
Everything about the run is kept in ``run_dir`` for later inspection and evals.
"""
from __future__ import annotations

import json
import re
import os
import signal
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .config import PACKAGE_PROMPTS, Agent

ACTIONS = ("fix", "wait", "ask", "none")


@dataclass
class Option:
    choice: str
    pros: str
    cons: str
    chosen: bool = False


@dataclass
class Decision:
    """The judgement call behind a fix or a question, laid out for a person."""
    question: str               # the one thing a person has to decide
    why: str                    # why the bot should not decide it alone (or, if routine, why no judgement is involved)
    options: list[Option]       # real alternatives; exactly one is chosen (what the PR does / what the bot recommends)
    routine: bool = False       # fix only: no judgement involved, may merge automatically

    @property
    def chosen(self) -> Option:
        return next(option for option in self.options if option.chosen)


@dataclass
class Result:
    action: str
    title: str
    summary: str
    details: str = ""
    decision: Decision | None = None
    duplicate_of: int | None = None


@dataclass
class Run:
    run_dir: Path
    result: object | None  # a Result for bot runs; whatever `parse` returns otherwise
    error: str | None = None
    cost_usd: float | None = None
    tokens: int = 0
    seconds: float = 0.0
    extra: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.result is not None


def render(template: str, **values: object) -> str:
    """Replace ``{{name}}`` placeholders in one pass; a placeholder without a value is an error."""
    def fill(match: re.Match) -> str:
        if match.group(1) not in values:
            raise ValueError(f"prompt placeholder {{{{{match.group(1)}}}}} has no value")
        return str(values[match.group(1)])
    return re.sub(r"\{\{(\w+)\}\}", fill, template)


def result_contract() -> str:
    return (PACKAGE_PROMPTS / "result.md").read_text()


def parse_result(text: str) -> Result:
    """Take the last JSON object in the agent's final message."""
    data = last_json_object(text)
    if data is None:
        raise ValueError("no JSON object in agent output")
    action = str(data.get("action", "")).strip().lower()
    if action not in ACTIONS:
        raise ValueError(f"action must be one of {', '.join(ACTIONS)}, got {action!r}")
    title = str(data.get("title") or "").strip()
    summary = str(data.get("summary") or "").strip()
    if not title or not summary:
        raise ValueError("result needs a title and a summary")
    duplicate = data.get("duplicate_of")
    decision = parse_decision(data.get("decision")) if action in ("fix", "ask") else None
    return Result(
        action=action,
        title=title[:120],
        summary=summary,
        details=str(data.get("details") or "").strip(),
        decision=decision,
        duplicate_of=int(duplicate) if isinstance(duplicate, (int, str)) and str(duplicate).isdigit() else None,
    )


def parse_decision(data: object) -> Decision:
    """A fix or a question must lay out its decision completely; anything less is an unusable result."""
    if not isinstance(data, dict):
        raise ValueError("fix and ask need a decision object")
    question, why = str(data.get("question") or "").strip(), str(data.get("why") or "").strip()
    if not question or not why:
        raise ValueError("decision needs a question and a why")
    options = []
    for item in data.get("options") or []:
        if not isinstance(item, dict):
            raise ValueError("each decision option must be an object")
        option = Option(*(str(item.get(k) or "").strip() for k in ("choice", "pros", "cons")),
                        chosen=item.get("chosen") is True)
        if not (option.choice and option.pros and option.cons):
            raise ValueError("each option needs a choice, pros and cons")
        options.append(option)
    if len(options) < 2:
        raise ValueError("decision needs at least two options")
    if sum(option.chosen for option in options) != 1:
        raise ValueError("exactly one option must be chosen")
    return Decision(question=question, why=why, options=options, routine=data.get("routine") is True)


def last_json_object(text: str) -> dict | None:
    decoder = json.JSONDecoder()
    found = None
    index = text.find("{")
    while index != -1:
        try:
            value, end = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            index = text.find("{", index + 1)
            continue
        if isinstance(value, dict):
            found = value
        index = text.find("{", end)
    return found


def run(agent: Agent, prompt: str, workdir: Path, run_dir: Path, parse=None) -> Run:
    """Run the agent; ``parse`` turns its final message into ``Run.result`` (default: a bot Result)."""
    parse = parse or parse_result
    run_dir.mkdir(parents=True, exist_ok=True)
    prompt_file = run_dir / "prompt.md"
    prompt_file.write_text(prompt)
    values = {"prompt": prompt, "prompt_file": str(prompt_file), "workdir": str(workdir), "run_dir": str(run_dir)}
    argv = [part.format(**values) for part in agent.command]
    uses_stdin = not any("{prompt" in part for part in agent.command)
    env = {name: os.environ[name] for name in agent.env if name in os.environ}

    started = time.monotonic()
    process = subprocess.Popen(
        argv, cwd=workdir, env=env, text=True, start_new_session=True,
        stdin=subprocess.PIPE if uses_stdin else subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(prompt if uses_stdin else None, timeout=agent.timeout_minutes * 60)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
    seconds = time.monotonic() - started
    (run_dir / "stdout.txt").write_text(stdout)
    (run_dir / "stderr.txt").write_text(stderr[-20000:])

    final_text, cost, tokens = _usage(agent.usage, stdout, run_dir)
    outcome = Run(run_dir=run_dir, result=None, cost_usd=cost, tokens=tokens, seconds=seconds)
    if timed_out:
        outcome.error = f"agent timed out after {agent.timeout_minutes} minutes"
    elif process.returncode != 0:
        outcome.error = f"agent exited with status {process.returncode}: {stderr.strip()[-500:]}"
    else:
        try:
            outcome.result = parse(final_text)
        except ValueError as exc:
            outcome.error = f"unusable agent result: {exc}"
    if outcome.result:
        data = asdict(outcome.result) if hasattr(outcome.result, "__dataclass_fields__") else outcome.result
        (run_dir / "result.json").write_text(json.dumps(data, indent=2))
    return outcome


def _usage(kind: str, stdout: str, run_dir: Path) -> tuple[str, float | None, int]:
    """Return (final message text, cost in USD or None, total tokens)."""
    if kind == "claude":
        # `claude -p --output-format json` prints one object with the answer and its cost.
        try:
            data = json.loads(stdout)
            usage = data.get("usage") or {}
            tokens = sum(int(usage.get(key) or 0) for key in (
                "input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"))
            return str(data.get("result", "")), data.get("total_cost_usd"), tokens
        except (ValueError, AttributeError):
            return stdout, None, 0
    if kind == "omp":
        # `omp -p --session-dir {run_dir}/session` writes JSONL records carrying usage.
        cost, tokens, seen = 0.0, 0, False
        for path in sorted((run_dir / "session").rglob("*.jsonl")):
            for line in path.read_text().splitlines():
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                usage = record.get("usage") or (record.get("message") or {}).get("usage")
                if not isinstance(usage, dict):
                    continue
                seen = True
                tokens += int(usage.get("totalTokens") or 0)
                total = (usage.get("cost") or {}).get("total") if isinstance(usage.get("cost"), dict) else None
                cost += float(total or 0)
        return stdout, (cost if seen else None), tokens
    return stdout, None, 0
