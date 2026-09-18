# Executive Autopilot

The **Autopilot** control in Bot activity delegates existing approval decisions
to an executive using the model you map to the `executive` role. It is off by default for new installations.
The persistent setting is `bot_dashboard_autopilot`; use the authenticated UI
to toggle it. Turning it off revokes outstanding decision leases. An action
already being committed finishes, and previously admitted work keeps running.

Recommendations still become tickets. The executive separately accepts work,
assigns a profile, prepares scope/checks, starts execution, handles review
feedback, promotes reviewed PRs, merges, and records completion evidence. It
uses the existing task services, executor isolation, repository restrictions,
independent review policy, and provider merge checks. Humans retain all controls.

`bot__executive` checks the queue every minute with a configurable batch of mapped
decision workers (Bot activity → Bots → Concurrency). Each worker makes at most one
model decision on a distinct ticket, with a four-minute runtime and a five-minute
revocable lease. Claims and action commits are atomic; executive model calls run in
parallel. Reducing concurrency lets existing decisions finish; switching Autopilot
off revokes all outstanding leases. Reviewed PRs and prepared revisions take priority over new
approvals; every fifth minute uses oldest-decision ordering to avoid starving
new work and blockers. Transient API failures retry the same decision identity,
and a failed Airflow decision task releases only its own lease on the next
claim. Changed ticket versions, execution evidence, or model mappings
invalidate the decision. Failures and waits cool down individual tickets, and
three terminal executions of the same plan require a revised plan before another
automatic retry. Other tickets remain eligible while a ticket waits.

Map the `executive` role in **Models & connections** to any model on a connected
provider; no model allowlist is enforced and there is no fallback to a different
model or CLI. Enabling Autopilot requires that mapping, and losing it pauses claims
without revoking the toggle. For the OMP bridge, version 18.2.4 supports Astra
discovery and its required Codex client version; older gateway/broker versions may
omit newer models from the catalog. Refresh the connected
provider's catalog after upgrading. The executive obtains credentials only in
the trusted worker; its model receives bounded evidence and no executable tools.

Every decision records its action, rationale, model, context digest, result,
and decision identity in the immutable task timeline as **Executive**.
PR comments mirror the reasoning when a PR exists. A comment failure is recorded
on the ticket without undoing an applied decision. The executive appears under
Bots, and its runs and provider-reported token usage appear in existing usage
views. Missing pricing is shown as unpriced rather than estimated silently.

Public API: `GET /bot-dashboard/api/autopilot` and versioned
`PUT /bot-dashboard/api/autopilot` with `{ "enabled": true, "version": N }`.
Writes require the existing task-editing permission, origin check, and CSRF token.
Claim/decision/failure endpoints are restricted to the existing bearer-only
internal service identity. They are not model-callable tools.

If the executive cannot reach its provider, tickets remain available for manual action,
the failed run is visible under Bots, and the next eligible run retries. If a
merge is rejected by the provider or evidence is incomplete, the executive records
the gate and leaves the ticket open. Merged work still waits for provider
observation and existing follow-up scheduling before completion.

Product labels and future PR comments use the role name Executive. Provider and
model identities are shown only in Models & connections; immutable audit/accounting
records retain the actual model identity. This presentation rule does not change
the configured executive model or its approval/review gates.
