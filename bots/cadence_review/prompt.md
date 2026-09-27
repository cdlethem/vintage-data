# Cadence usefulness review

Audit only. Do not edit cadence or source configuration. Review every supplied flagged source and the bounded rotating sample.

A task_proposal must contain recommendation_key, title, category, priority, planned_resolution, why_now, expected_benefit, risk, rollback, verification_commands (argv arrays, never shell strings), allowed_path_globs, resource_keys, follow_up_bots, suggested_executor (junior|senior|staff), reviewer_required=true, and evidence entries {kind,reference,summary}.

Return one `CadenceReviewV2` JSON object: schema_version=2, agent="cadence_review", status, source_reviews {source,decision keep|adjust|observe|human_review,reason}, issues, task_proposals, and summary. Propose tasks only for evidence-backed adjustments. Put a bounded correction on matching existing work; do not open a new ticket for reworded evidence.

## Context
{{CADENCE_CONTEXT}}

In each `source_reviews.reason`, `issues` entry, proposal, and `summary`, name
the affected data source and explain its current collection frequency, the
observed freshness or wasted-work symptom, and the consequence for consumers.
Separate measured history from an unverified forecast; explain why the evidence
supports keeping, observing, adjusting, or requesting human review. For an
adjustment, state the recommended schedule change, the concrete approval or
decision needed, its benefit and risk, and how to check and reverse it. For
observation, say what measurement and threshold would change the decision.
Lead with plain language; leave cron expressions, timestamps, run IDs, and
commands as supporting details. Do not describe historical checks as repairs.
Start `planned_resolution` with one or two plain sentences naming the source,
the observed schedule symptom, and exact recommended change or decision;
put detailed evidence afterward. Do not call routine pending validation a
human decision.

Use a short action title naming the dataset or service and intended change.
Keep snake_case identifiers, timestamps, hashes, and run IDs in evidence, not
titles. Reuse existing backlog work when it covers the same issue; keep
resource_keys stable across reports. A new report or reworded recommendation
is not a new issue.
