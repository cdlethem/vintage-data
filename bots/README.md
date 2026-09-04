# bots

Recurring **agent** invocations against this project, defined as data. A bot is
a directory:

```
bots/<name>/
  bot.yml      what to run, when, with which model alias, and when to skip
  prompt.md    the prompt, with {{PLACEHOLDER}}s filled from context commands
```

`bot_runner.py` is the only code that runs them, `providers.py` is the only code
that talks to a model, and `orchestration/dags/bots_dag.py` turns each `bot.yml`
into an Airflow DAG (`bot__<name>`). Adding a bot is adding a directory — the
same trade the extract layer makes with `extract/sources/*.yml`.

## Why it is shaped this way

Three things get confused in most "AI job" setups; here they are separate:

| concern | lives in | tracked? |
|---|---|---|
| **what the bot does** — prompt, cadence, evidence, skip rule | `bots/<name>/` | yes |
| **which model answers** — vendor, endpoint, model id, limits | `bots/models.yml` | no (per machine) |
| **credentials** | `orchestration/airflow.secrets.env` | no |

A bot names a *model alias*, never a vendor. Point the alias somewhere else and
every bot follows; no bot definition changes when you move from a GPU under your
desk to OpenRouter.

## Model selection

`bots/models.yml` (copy from `models.example.yml`) maps aliases to providers:

```yaml
default: openrouter          # used by any bot that names no model

models:
  openrouter:
    provider: openai_chat
    endpoint: https://openrouter.ai/api/v1/chat/completions
    model: anthropic/claude-sonnet-4.5
    api_key_env: OPENROUTER_API_KEY     # the *name* of the env var, not the key
    max_tokens: 2000
  local:
    provider: openai_chat
    endpoint: http://127.0.0.1:8080/v1/chat/completions
    model: local-model
    preflight:                           # non-zero exit = "busy", skip cycle
      command: ["bots/bin/free_slots", "http://127.0.0.1:8080/slots"]
      cwd: .
```

Providers (`providers.py`), all stdlib-only so a worker needs no vendor SDK:

| provider | speaks to |
|---|---|
| `openai_chat` | anything OpenAI-compatible: OpenRouter, OpenAI, Groq, Together, DeepSeek, vLLM, llama-server, Ollama |
| `anthropic_messages` | Anthropic's native `/v1/messages` |
| `command` | any local program — a coding-agent CLI, a wrapper script, a mock. Prompt on stdin (or `{prompt}` in `argv`), report on stdout, exit 3 = busy |

Choosing per run is first-class in every entry point:

```bash
P=orchestration/.venv/bin/python

$P bots/bin/run_bot --list                              # bots, cadence, model
$P bots/bin/run_bot pipeline_check --dry-run            # prompt only, no model
$P bots/bin/run_bot pipeline_check                      # the bot's own alias
$P bots/bin/run_bot pipeline_check --model openrouter   # override the alias
$P bots/bin/run_bot pipeline_check --models-config /tmp/experiment.yml
```

An unknown alias fails loudly and names the aliases that do exist; a missing API
key names the environment variable to set. Neither silently falls back to a
different model — you always know which model produced a report.

## Anatomy of a bot.yml

```yaml
name: pipeline_check
description: assess sources that deterministic triage flagged
schedule: "*/30 * * * *"     # five-field UTC cron, or "manual"
enabled: true                # false => DAG created paused
model: default               # alias in models.yml
timeout_minutes: 15
keep_report_days: 30

context:                     # deterministic evidence, gathered before the model
  DIGEST:
    command: ["../../orchestration/.venv/bin/python", "digest.py", "--triage"]
    cwd: monitoring          # relative to the repo root
gate:                        # end the run before any model call
  context: DIGEST
  skip_if_matches: "(?m)^STATUS: OK"
  skip_report: "VERDICT: OK — triage clean, no model call."
```

Rules the runner enforces:

- Every `{{KEY}}` in the prompt must have a context entry; an unfilled
  placeholder is an error, never a prompt sent with literal braces in it.
- A context command that exits non-zero fails the run. Evidence is not optional.
- The gate is checked *before* the model is resolved, so a healthy pipeline
  costs zero tokens and needs no credentials at all.
- Reports land in `bots/runs/<name>/<utc-ts>.md` (gitignored) with a header
  naming the bot, alias and provider that produced them.

Outcomes: `ok` (model answered), `skipped` (gate), `busy` (preflight or the
provider said "later"). The Airflow task stays green for all three and fails
only on broken wiring — a saturated GPU is not an incident.

## Live bots

| bot | cadence | what it does |
|---|---|---|
| `pipeline_check` | every 30 min | `monitoring/digest.py --triage` classifies every source deterministically; the model is asked only about flagged ones and returns DISMISS / WARN / INVESTIGATE per source |

## Planned bots

Both are deliberately *not* stubbed out — they need work this repo hasn't done
yet, and an empty directory would be a lie about what runs. What they will be:

- **`script_staging`** — walk `discovery/possible_sources/` backlog items,
  produce a `fetch_<slug>.py` into `discovery/staged_scripts/`, and let the
  existing gate (`check_item.py`) decide whether it may stay. Needs the vetting
  harness itself to be part of this repo rather than gitignored local work;
  until then it runs by hand.
- **`schedule_tuning`** — read `.meta.json` record counts per source over time
  and propose cron changes for sources that poll faster than they change. Needs
  `(source, id)` dedupe in the load layer to tell "new data" from "same data
  again"; that is the load stage's next real feature, not a prompt.

Both slot in as directories under `bots/` with no code change here.
