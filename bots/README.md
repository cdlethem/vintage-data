# airflow-bots

Self-healing agents for Airflow pipelines. When a task keeps failing, a coding agent reads the log and the code, then either opens a pull request with a fix, or files a plain-language issue saying what's wrong and what it needs from you. Everything it does is a GitHub issue, pull request, label, or comment, so you manage it with the tools you already use.

It is a small Python package (`pyyaml` is the only dependency) plus a DAG file. It brings no database, web UI, or services of its own.

## How it works

```
every 15 min, DAG bots_heal:
  plan   ── close issues whose task recovered
         ── merge bot PRs labelled bots:automerge once CI is green
         ── find tasks that are failing now, or fail often, and have no issue yet
         ── find unanswered "/bot ..." comments
  work[] ── one agent run per item, in a fresh git worktree
              fix  -> commit, push bots/<issue>-<slug>, open PR "Fixes #N"
              wait -> issue that closes itself when the task succeeds again
              ask  -> issue with one concrete question for you

on a cron, DAG bots_job__<name>:
  run    ── one agent run with your prompt (e.g. "add a new data source") -> PR or issue
```

The agent is any command-line coding agent (omp, Claude Code, Codex, aider, or your own wrapper). The bot hands it a prompt and a checkout, then reads one JSON object from its final message:

```json
{"action": "fix | wait | ask | none", "title": "...", "summary": "...", "details": "...",
 "question": "...", "review": true, "duplicate_of": null}
```

The bot itself commits and pushes. The agent never receives the GitHub token.

## Working with the bot on GitHub

| You see | Meaning | What you can do |
|---|---|---|
| issue `bots:waiting` | Nothing to change in code (e.g. upstream outage). | Nothing. It closes itself once the task is healthy again. |
| issue `bots:question` | The bot needs a decision. | Reply `/bot <answer>`. |
| issue `bots:review` + PR | A fix that should be reviewed. | Review and merge, or comment `/bot <change request>` on the PR. |
| issue `bots:fixing` + PR `bots:automerge` | A low-risk fix that merges once all checks pass. | Remove `bots:automerge` to stop it. |
| any issue | | Close it when it's handled. Add `bots:mute` to make the bot ignore that task for good. |

- `/bot <request>` works on any issue and on the bot's own PRs, from people with write access (owner, member, collaborator). The bot replies once per request.
- A task is tracked by a hidden `<!-- bots:key=failure:<dag_id>/<task_id> -->` marker in the issue body (`flaky:` for a task that fails intermittently). A `failure:` issue counts as healthy once the latest run succeeds; a `flaky:` issue once the task has gone a full lookback window without failing. If the bot decides two failures share a root cause, the second task's marker is added to the first issue.
- After an issue closes, failures within the next hour are treated as the old problem (a merged fix needs time to deploy). A later failure gets a new issue.
- Auto-merge requires at least one CI check on the PR; with no CI configured the bot never merges.

## Setup

1. Install into the Airflow environment: `pip install airflow-bots` (or `pip install -e path/to/bots`).
2. Write a config file (below) and point `BOTS_CONFIG` at it for the scheduler, DAG processor and workers.
3. Add a DAG file:

   ```python
   from airflow_bots.dags import build
   globals().update(build())
   ```

4. Provide secrets as environment variables on the workers:
   - `GITHUB_TOKEN`: a token that can read/write contents, issues and pull requests of the repository.
   - Airflow API access to read failed task instances and their logs: `BOTS_API_USERNAME` and `BOTS_API_PASSWORD` for a user that can obtain a token from `/auth/token` (for Simple Auth, role `op` is enough), or a pre-issued token via `airflow.token_env`.
5. Try it by hand before unpausing the DAG:

   ```bash
   airflow-bots sweep --dry-run                        # what would the bot do right now?
   airflow-bots heal <dag_id> <task_id> --dry-run      # run the agent, print the issue/PR instead of creating it
   ```

## Configuration

```yaml
github:
  repo: acme/pipelines          # owner/name
  base: main                    # branch fixes are based on and merged into
  token_env: GITHUB_TOKEN       # default
  api_url: https://api.github.com   # GitHub Enterprise: https://ghe.example.com/api/v3
  author: "airflow-bots <airflow-bots@users.noreply.github.com>"   # commit author

airflow:
  url: http://localhost:8080    # REST API base used by the workers
  ui_url: https://airflow.example.com   # for links in issues (default: url)
  username_env: BOTS_API_USERNAME       # defaults; or token_env: AIRFLOW_TOKEN
  password_env: BOTS_API_PASSWORD

state_dir: /var/lib/airflow-bots  # git clone, worktrees, run records, spend ledger
instructions: bots-notes.md       # optional project notes appended to every prompt

agents:                           # one or more; "default" is used unless you say otherwise
  default:
    command: [claude, -p, --output-format, json, --permission-mode, acceptEdits]   # prompt on stdin
    timeout_minutes: 30
    env: [HOME, PATH, ANTHROPIC_API_KEY]   # only these variables reach the agent
    usage: claude                          # read cost from the output: claude | omp | none

limits:
  daily_runs: 30                  # agent runs per UTC day, across all bots
  daily_usd: 10                   # stop starting runs once reported spend reaches this
  concurrency: 2                  # parallel agent runs in one sweep

heal:
  schedule: "*/15 * * * *"
  lookback_hours: 24              # failures older than this are ignored
  max_new_per_sweep: 3            # new failures investigated per sweep (busiest first)
  flaky_streaks: 3                # also investigate a task whose latest run passed but which failed
                                  # this many separate times in the window (null: current failures only)
  ignore: ["*.sensor_*"]          # fnmatch on dag_id or dag_id.task_id; the bot's own DAGs are always ignored
  agent: default
  prompt: my_heal_prompt.md       # optional replacement for the built-in prompt
  auto_merge:
    enabled: false                # merge only if the agent says review isn't needed,
    paths: ["dags/sources/*"]     # every changed file matches, and CI is green

jobs:                             # optional scheduled agents
  new_source:
    schedule: "0 6 * * 1"
    prompt: jobs/new_source.md
    agent: default
    enabled: true                 # initial pause state of the DAG
    auto_merge: {enabled: false}

evals:
  cases: evals/                   # eval case files
  judge: default                  # agent that grades eval runs
  db: evals.sqlite                # default: <state_dir>/evals.sqlite
```

Relative paths resolve against the config file. `$VAR`/`${VAR}` are expanded; an unset variable is an error.

### Agent commands

Placeholders in `command`: `{prompt}` (prompt text as an argument), `{prompt_file}` (path to it), `{workdir}` (the checkout, also the working directory), `{run_dir}` (a private directory for this run). Without `{prompt}`/`{prompt_file}` the prompt goes to stdin.

```yaml
# omp with any model or role it knows; usage is read from the session files
command: [omp, -p, --model, "@default", --auto-approve, --session-dir, "{run_dir}/session", "{prompt}"]
usage: omp

# Claude Code
command: [claude, -p, --output-format, json, --permission-mode, acceptEdits]
usage: claude

# Codex (usage not read; limits.daily_runs still applies)
command: [codex, exec, --full-auto, "{prompt}"]
usage: none
```

To use your own inference provider, configure the agent CLI for it (e.g. an OpenAI-compatible base URL, a local llama.cpp or vLLM server) and pass its credentials through `env`.

The agent runs as the Airflow worker's user with the listed environment variables only. It can run commands in its checkout and reach the network. If that's more trust than you want, wrap the command in a container or sandbox (`[docker, run, --rm, -v, "{workdir}:/work", ...]`, `bwrap ...`) and keep credentials the agent doesn't need out of `env` and out of `HOME`.

## Spend and monitoring

- Every agent run is appended to `<state_dir>/ledger.jsonl` (subject, action, cost, tokens, duration, issue/PR). Limits are checked before each run; once reached, work tasks are skipped (visible as skipped in Airflow) until midnight UTC.
- `airflow-bots status` prints today's runs and spend against the limits and the recent run list.
- Airflow shows each agent run as one mapped `work` task instance named after the failing task, with its log.
- Each run's prompt, raw output, result, and diff are kept in `<state_dir>/runs/<run-id>/`.

Multiple Airflow hosts: the ledger and git clone live in `state_dir`, so put it on shared storage or route the bot DAGs to one worker queue.

## Improving prompts with evals

Prompts are meant to be replaced and compared, not hand-tuned case by case. The loop:

1. **Capture.** Any production run can become a test case: `airflow-bots eval capture <run-id>` writes `evals/<id>.yml` with the exact input the agent saw and the commit it ran on.
2. **Describe good.** Fill in the case: deterministic `expect` checks and a short rubric for the judge.

   ```yaml
   id: pokeapi-pagination
   kind: heal                     # heal | request | job:<name>
   base_sha: 3f2a...              # the repository state to replay against
   expect:
     action: fix                  # or a list: [fix, ask]
     paths: ["extract/scripts/*", "extract/test_*.py"]   # changed files must match
     forbid: ["extract/sources/*"]
   rubric: |
     - Identifies that the API's `next` link format changed.
     - Updates the pagination check instead of disabling it.
     - Runs the fetcher's tests and reports the result.
   variables: {...}               # recorded input; normally left as captured
   ```

3. **Run.** `airflow-bots eval run --label "shorter heal prompt"` replays every case at its commit with the current prompts, applies the checks, and has the judge agent grade each rubric line. `--prompt candidate.md` tries a different template, `--agent <name>` a different model, `--repeat 3` measures consistency, `--parallel 2` runs cases side by side.
4. **Compare.** `airflow-bots eval report` lists runs with pass rates and a hash of the prompt text they used; `eval show <run>` shows unmet rubric lines; `eval compare <a> <b>` puts two runs side by side per case.

Results are stored in SQLite (`runs` and `results` tables) for your own queries. Eval runs are not counted against production limits.

## Design notes

- **GitHub is the tracker.** Issues, PRs, labels, and comments already provide state, history, search, notifications, permissions, and a UI. The bot stores no ticket state of its own; hidden markers in bodies tie issues to tasks.
- **Airflow's REST API is the failure feed**, so any deployment and log backend works without changes to your DAGs.
- **One agent run per problem, one contract.** Diagnosis and fixing are the same run; the result format is the same for healing, `/bot` requests, and jobs.
- **Merge safety comes from CI and branch protection**, plus an explicit path allow-list for automatic merges. Anything else waits for a person.
- **Budgets are coarse on purpose**: a daily run count and a daily reported spend. Per-run limits belong to the agent command (`--max-time`, `--max-turns`) and `timeout_minutes`.
- Only GitHub is supported. Other trackers would replace `github.py`; nothing else talks to GitHub.
