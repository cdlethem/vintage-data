# Integration evidence

Verified on 2026-09-07 on x86-64 Linux using the pinned Lightdash 2.140.0
server/CLI, Postgres 18.6 and MinIO images in `images.env`.

- Parsed the complete dbt project and passed its manifest policy.
- Covered 163/163 mart models with 326 charts and 162 family dashboards.
- Checked every content file against the pinned native JSON schemas and checked
  deterministic regeneration without differences.
- Compiled all 163 explores against actual Postgres serving relations.
- Bootstrapped a local OSS organization/account, deployed a project, uploaded all
  content and completed Lightdash content/catalog validation without errors.
- Executed all 326 saved charts through the v2 asynchronous query API: zero failures.
- Created a preview, uploaded/validated its content and verified its project UUID
  differs from the retained deployment project UUID.
- Passed 12 snapshot/Postgres tests, including exact decimal/Unicode/null/timestamp
  round trips, updates/deletes/empty tables, idempotence/stale retries, schema-change
  rejection and rollback of both earlier tables and ledger on a later COPY failure.
- Passed 4 content/bundle tests and 19 bot-context/transform-runner tests.
- Rendered configuration twice from a different checkout with spaces in its path
  and state directory; the second `--check` was clean. Shell syntax and diff
  whitespace checks passed.

The database integration fixtures are synthetic rows derived from every mart's
contract, explicitly marked in their manifest and confined to a disposable `/tmp`
stack on separate ports. This verifies wiring, types and executable queries; it
is not a production data reconciliation or visual browser review. The production
feature flag remains opt-in and the production warehouse was not rebuilt or
published during these integration checks. Follow README setup/publication steps
for the live installation.

Failures found and fixed during integration: an upstream OSS-only migration issue
in the initially considered Lightdash 2.119.0 release (upgrade pin to 2.140.0),
registration request shape, profile template quoting, local Postgres SSL settings,
required internal worker enablement, overlong PostgreSQL field aliases, and JSON
grouping expressions. Static Lightdash validation did not catch the latter query
errors; retain `viz query-check` in upgrade and release verification.
