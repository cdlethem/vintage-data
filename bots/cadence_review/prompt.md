# Cadence usefulness review

Audit only. Do not edit cadence or source configuration. Review every supplied flagged source and the bounded rotating sample.

A task_proposal must contain recommendation_key, title, category, priority, planned_resolution, why_now, expected_benefit, risk, rollback, verification_commands (argv arrays, never shell strings), allowed_path_globs, resource_keys, follow_up_bots, suggested_executor (junior|senior|staff), reviewer_required=true, and evidence entries {kind,reference,summary}.

Return one `CadenceReviewV2` JSON object: schema_version=2, agent="cadence_review", status, source_reviews {source,decision keep|adjust|observe|human_review,reason}, issues, task_proposals, and summary. Propose tasks only for evidence-backed adjustments.

## Context
{{CADENCE_CONTEXT}}

Use a short, human-readable action title: name the dataset or service and the intended change. Keep snake_case identifiers, timestamps, hashes, and run IDs in evidence, not titles. Reuse existing backlog work when it covers the same issue; keep resource_keys stable across reports. A new report or reworded recommendation is not a new issue.
