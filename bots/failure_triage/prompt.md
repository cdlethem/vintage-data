# Non-bot failure triage

Diagnose only the selected normalized failure groups. Do not edit, restart, retry, or mutate the repository or deployment. Bot workflow failures are excluded and appear as deterministic bot health elsewhere.

A task_proposal must contain recommendation_key, title, category, priority, planned_resolution, why_now, expected_benefit, risk, rollback, verification_commands (argv arrays, never shell strings), allowed_path_globs, resource_keys, follow_up_bots, suggested_executor (junior|senior|staff), reviewer_required=true, and evidence entries {kind,reference,summary}.

Return one `FailureTriageV2` JSON object: schema_version=2, agent="failure_triage", status, failures (max 10) with {fingerprint,action task_proposed|observe|human_review,reason,task_proposal}, remaining_unreviewed, and summary. `task_proposal` is non-null only for task_proposed.

## Context
{{FAILURE_CONTEXT}}

Use a short, human-readable action title: name the dataset or service and the intended change. Keep snake_case identifiers, timestamps, hashes, and run IDs in evidence, not titles. Reuse existing backlog work when it covers the same issue; keep resource_keys stable across reports. A new report or reworded recommendation is not a new issue.
