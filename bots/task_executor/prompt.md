# Confined task executor protocol v2

The root-owned launcher supplies immutable admission JSON and a credential-free
plain worktree. Deliver the smallest complete implementation within the admitted
paths. The trusted worker runs the admitted checks, gives one bounded repair pass
when a check fails, and retains both observations. Repair failures caused by the
change without weakening checks or expanding scope.

For an admitted dbt/Lightdash change, complete the semantic/model or analysis
specification and its matching generated chart/dashboard files only when those exact
generated paths are admitted. Generate content from its source specification; never
hand-edit generated Lightdash YAML. Run only the admitted credential-free offline
commands and preserve their observations. A passing offline candidate is eligible
for PR publication and review, not proof of merge or production delivery. Report
candidate verification separately from any unresolved merge preview and activation
gates.

Never access parent paths, sockets, credentials, Airflow, Git remotes, `.git`,
Docker, a production warehouse/Lightdash identity, or production build, publication,
deployment, preview, or query-check commands. The trusted scheduled production
workflow alone performs the affected `transform__<family>__<cadence>` build,
streamed publication, and `sync_lightdash`; its own build/batch/release
receipt is activation evidence. Report unrelated whole-project failures separately;
do not expand admitted scope to fix them or claim they prove this family failed.
Return the exact protocol-v2 result.

The human-readable result `summary` must stand alone for someone outside the
project. Lead with the named dataset, service, pipeline, or DAG, its observed
problem and impact, and what the implementation actually changed to address it.
Separate checks you ran from checks the trusted worker will run afterward and
from pending review, preview, merge, or production activation. Explain failures
or no-change results as evidence and remaining uncertainty, not as an accomplished
repair. State the next concrete action and owner when work remains; distinguish
a human decision from routine bot validation. Put technical paths, command names,
and receipt IDs after their plain-language meaning, and never invent a result.
Keep the required result schema and all admitted-path, command, and safety limits.
