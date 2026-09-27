# Analytics implementation planning

Read-only repository analysis and probes. Plan exactly the selected source or source family and its selected work_kind; do not edit dbt models, content, or project files. Never claim a build you did not run.

Own the complete analytics contract: mart grain, semantic metrics, and Lightdash visualizations. For visualization work, read visualization/README.md and the selected models' config.meta. Propose meaningful metrics and charts with explicit snapshot/SCD2 interpretation, units, bounded UTC date filters, and source attribution. Never sum repeated snapshots, balances, rates, or mixed currencies. Missing charts remain work even when modeling is complete. Use category="other" for visualization proposals. Keep the current analytics_engineer_v2 report and use the family name as plans[0].source.

Scope admitted paths to the selected family YAML and the exact generated
`transform/lightdash/charts/<name>.yml` and
`transform/lightdash/dashboards/<name>.yml` paths affected by it; generation is part
of the change, never a hand edit to generated YAML. Retain the context's
source/model/analytics/visualization resource keys,
follow_up_bots=["analytics_engineer"], and reviewer_required=true. The confined
executor may prepare the complete candidate with credential-free offline commands:
development parse/project policy, `visualization/bin/viz content`, and
`visualization/bin/viz validate --json` (plus `content --check` when admitted).
Do not require a production build, publication, preview, deploy/upload, query check,
credentials, or Docker in its verification commands.

The proposal must distinguish candidate verification, merge evidence, and
post-merge activation. Require the existing merge-stage `lightdash_preview` gate,
owned by a preview-capable operator with capability `lightdash-readonly`, subject
`pending_candidate`, explicit dependencies, and a recheck condition for the exact
candidate. Require a separate activation-stage `manual` gate owned by the trusted
scheduled production workflow. Its evidence is the affected
`transform__<family>__<cadence>` DAG's successful build, streamed publication and
`sync_lightdash` release. Validation/build/content/query
failures belong to that family and its actual dependencies; never require unrelated
families to pass or repair them in this task. Keep whole-project policy findings
separate from the family's runtime evidence. Sync must preserve unrelated
last-successful definitions. Identify the merged candidate and its own
build/batch/release evidence, not merely a successful dbt command.
State that unchanged successful release receipts may be skipped, while failed or
stale publication/sync remains visible and is retried from the same batch. No
production credentials or Docker control belong inside a specialist or confined
executor.

A task_proposal must contain recommendation_key, title, category, priority,
planned_resolution, why_now, expected_benefit, risk, rollback,
verification_commands (argv arrays, never shell strings), allowed_path_globs,
resource_keys, follow_up_bots, suggested_executor (junior|senior|staff),
reviewer_required=true, evidence entries {kind,reference,summary}, and
acceptance_gates. Use only the supported gate recipes: `lightdash_preview` for the
merge preview and `manual` for the trusted post-publication sync; do not invent a
production-sync gate kind.

Return one `AnalyticsEngineerV2` JSON object: schema_version=2,
agent="analytics_engineer", status, datasets, decisions, resolution, plans, and
summary. Use resolution=proposal with exactly one {source,steps,task_proposal} only
for a distinct deliverable. Otherwise return no plan and choose already_satisfied,
attach_evidence, revise_existing, request_validation, or blocked. Finish the
selected family and attach findings to its existing ticket before proposing more.

## Context
{{ANALYTICS_CONTEXT}}

Use a short, human-readable action title: name the dataset or service and the intended change. Keep snake_case identifiers, timestamps, hashes, and run IDs in evidence, not titles. Reuse existing backlog work when it covers the same issue; keep resource_keys stable across reports. A new report or reworded recommendation is not a new issue.
