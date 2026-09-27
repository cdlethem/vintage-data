# Trusted Git publication setup

The API and worker publish reviewed executor artifacts using an encrypted Airflow
Connection named `bot_dashboard_git`. Credentials stay outside the executor sandbox.
Provisioning requires the rendered Airflow environment and its Fernet key. Enable
`AIRFLOW__BOT_DASHBOARD__ALLOWED_GIT_HOSTS=api.github.com,github.com` for public GitHub;
self-hosted GitHub/GitLab requires its own explicit host allowlist.

Use a dedicated service-account token with repository read/write and pull-request
(or merge-request) permissions. The command checks authenticated account identity
and repository push permission before saving; provider-side branch rules and PR
permissions can still reject a later publication. It never merges.

```bash
# GIT_PUBLISH_TOKEN is injected by your secret manager; its value is never an argument.
bot-dashboard provision-git \
  --provider github --project cdlethem/vintage-data \
  --api-base-url https://api.github.com \
  --clone-url https://github.com/cdlethem/vintage-data.git \
  --base-branch main --service-account-id ACCOUNT_NUMERIC_ID \
  --allow-path 'extract/scripts/**' --allow-path 'extract/sources/**' \
  --allow-path 'extract/test_*.py' --allow-path 'transform/models/**' \
  --allow-path 'transform/lightdash/**' \
  --deny-path '**/*.env' --deny-path '**/secrets/**' \
  --token-env GIT_PUBLISH_TOKEN
# Repeat with --apply to persist the validated configuration.
```

`--token-file /private/token` requires a regular owner-only file owned by the
invoking user; `--token-file -` reads a token from stdin. No token-value argument
is supported. The default is a read-only validation preview; `--apply` creates or
rotates the connection transactionally. Include every admitted task path in the
repository allowlist; task scopes further narrow those permissions. Limits default
to 40 files and 500,000 diff bytes.

For GitLab use `--provider gitlab`, the canonical API base (usually ending in
`/api/v4`), its HTTPS clone URL, and the numeric service-account ID. API and clone
hosts must match except for the standard public GitHub API/clone pair.

## Credential lifecycle

The current provider reads a static encrypted password from Airflow's connection
resolver. It does not mint or refresh GitHub App installation tokens. This helper
validates user/service-account tokens through the provider's `user` endpoint;
it does not provision installation tokens. Re-run it with a replacement account
token before expiry. A secret-manager-backed Airflow connection can supply rotating
account credentials without writing the metadata database; configure that backend
separately and avoid shadowing a database connection of the same name.

GitHub App installation authentication needs a trusted token broker and corresponding
installation identity/repository-permission validation before it can be supported.
Keep App private keys and any future broker outside the sandbox. Do not substitute
an installation token into this account-token provisioning flow or claim automatic
refresh exists.
