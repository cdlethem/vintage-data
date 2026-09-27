# Model connections and bot mappings

Open **Bot activity → Models & connections** to connect an OpenAI-compatible provider. Enter its
API base URL (for example `https://provider.example/v1`) and API key, test the
connection, then save it. The connection test retrieves the provider's `/models`
list without running a completion. Choose a provider and model for each bot or
executor profile and save the mappings.

Keys are encrypted using Airflow's Fernet key in `bot_dashboard_model_<id>`
Connections. Browser reads return only `has_api_key`. Leaving the key blank keeps
an existing credential; changing its destination requires entering the key again.
The nonsecret mappings live in the `bot_dashboard_model_assignments` Variable.
Existing local CLI configuration is displayed as the current model for unmapped
roles. A connected provider must support OpenAI-compatible model listing and chat
completion APIs; connecting it does not grant repository or web tools by itself.

HTTP and private-network gateways require an explicit operator allowlist in the
API server's environment:

```text
BOT_DASHBOARD_MODEL_ALLOWED_HOSTS=127.0.0.1:8080,model-gateway.internal:8000
```

Entries are exact host and port pairs. Public providers require HTTPS and public
DNS addresses. Redirects are disabled. API keys are never encoded into URLs.

The bearer-authenticated internal `GET /bot-dashboard/api/internal/model-settings` returns
credentials only to the trusted bot service parent so it can construct a scoped
model gateway. The parent must keep them out of prompts, reports, admission files,
and sandbox environments. Server-side `model_for_role(session, role)` returns only
provider ID, model, and base URL for an admission snapshot. OMP's existing command
configuration remains distinct from a connected HTTP API; the trusted gateway
bridge supplies HTTP access without embedding provider keys in model instructions.
