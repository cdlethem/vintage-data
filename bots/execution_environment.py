"""Shared execution facts for planners and the executive; no model-specific policy."""

VERIFICATION_ENVIRONMENT = """Execution environment and verification requirements:
Task executors and independent reviewers run in confined sandboxes without general
network access, external DNS, Git metadata, host credentials, production Airflow access, or a live
warehouse/Lightdash identity. Their verification_commands must be executable offline:
behavioral tests with deterministic HTTP fixtures, compilation, development parse,
and repository/content checks. A live extractor invocation cannot succeed there.
Shared-checkout bootstrap wrappers that require git-common-dir or host environments
are unavailable too. Prefer direct equivalent offline commands where applicable;
retain database-backed synchronization as an external gate when it needs live data.
The sandbox PATH supplies the installed runtime Python and tools. Use python/python3
from PATH for admitted checks; checkout-local virtualenvs such as
orchestration/.venv/bin/python are not included in the source archive.
Verification wrappers must preserve bounded, redacted child stderr on failure.
Do not capture stderr and then expose only CalledProcessError; retain the exit status
and useful failure category without printing credentials or authenticated URLs.

Keep required live-source smoke checks, warehouse builds, previews and activation
checks as explicit separate acceptance gates in the plan, to be fulfilled through
a named capable bot or authorized operator workflow with actual recorded evidence.
Do not delete, weaken, or claim completion of those gates when moving their commands
out of sandbox verification. If no approved validation path or evidence is available,
retain the corresponding blocker. Fixture success never proves live-source success.
Before starting an existing ticket, configure a plan that distinguishes sandbox
checks from external evidence requirements. Sandbox DNS failures are an environment
mismatch, not evidence of a temporary upstream outage; do not retry them unchanged.

File scope must satisfy BOTH the ticket's allowed_path_globs and the configured
repository_policy allowed_path_globs, with repository denied_path_globs taking
precedence. Ticket scope never overrides repository policy. Before admission,
check planned implementation AND test file locations against both boundaries.
When equivalent behavioral tests can live within permitted directories, configure
those locations and matching verification commands without reducing test coverage.
If required work cannot fit the policy, retain an explicit policy blocker instead
of widening repository permissions or retrying an unchanged rejected path.

Plan the executor handoff explicitly: its deliverable is the implementation plus
admitted offline checks for a reviewable PR, not final ticket completion. State in
planned_resolution which gates apply before PR publication, before merge, and before
activation/completion. Independent review and merge are downstream of the executor;
their absence alone must not prevent a finished implementation from reaching review.
When the approved plan defers live checks to a later stage, the executor should report
completed implementation and list those unresolved gates as remaining limitations,
not label its implementation BLOCKED solely because later stages have not run.
Actual implementation failures, failed admitted checks, scope violations, or missing
evidence explicitly required before publication remain blockers. Never relabel an
existing blocked result or waive a gate: the Executive must configure/approve the staged plan
and the executor must produce a fresh report under that admission.
In human-facing ticket text, translate these rules into the affected source or
pipeline's actual symptom, why a reader should care, the evidence already obtained,
and the specific next check or authorization and its owner. Keep stages, commands,
path globs, and gate keys as supporting technical details, not the whole explanation.
Never claim a source is working merely because its code passed offline tests.
"""
