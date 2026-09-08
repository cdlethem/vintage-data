"""Airflow provider metadata. This module must remain side-effect free."""

_OPTIONS = {
    "enabled": ("boolean", "False", "Register the bot dashboard API and UI."),
    "widget_enabled": ("boolean", "False", "Show the action queue on the Airflow dashboard."),
    "write_enabled": ("boolean", "False", "Enable authorized task mutations."),
    "allow_simple_auth_writes": ("boolean", "False", "Development-only SimpleAuthManager write override."),
    "csrf_cookie_secure": ("boolean", "True", "Set Secure on the double-submit CSRF cookie."),
    "executor_enabled": ("boolean", "False", "Enable isolated task execution admissions."),
    "internal_user": ("string", "bot-worker", "Bearer-authenticated task service username."),
    "git_conn_id": ("string", "bot_dashboard_git", "Airflow Connection holding repository settings and token."),
    "report_max_bytes": ("integer", "262144", "Maximum serialized projected report size."),
    "report_retention_days": ("integer", "3650", "Compatibility report retention ceiling."),
    "git_sync_minutes": ("integer", "5", "Provider state cache refresh interval."),
    "executor_pool": ("string", "bot_dashboard_executor", "Dedicated executor Airflow pool."),
    "executor_queue": ("string", "bot_dashboard_executor", "Dedicated executor Celery queue."),
    "max_queued_executions": ("integer", "20", "Maximum outstanding task execution admissions."),
    "allowed_git_hosts": ("string", "", "Comma-separated exact HTTPS Git provider hosts."),
    "artifact_root": ("string", "", "Absolute content-addressed artifact directory."),
    "daily_spend_cap_usd": ("float", "0.0", "Daily model spend cap in USD; 0 disables the cap."),
}


def get_provider_info() -> dict:
    return {
        "package-name": "apache-airflow-provider-vintage-bot-dashboard",
        "name": "Vintage Bot Dashboard",
        "description": "Bot activity, reports, and a durable manager action queue.\n",
        "versions": ["0.2.0"],
        "db-managers": [
            "airflow.provider.vintage.bot.dashboard.db_manager.BotDashboardDBManager"
        ],
        "config": {
            "bot_dashboard": {
                "description": "Vintage bot dashboard provider settings.",
                "options": {
                    name: {
                        "description": description,
                        "version_added": "0.2.0",
                        "type": kind,
                        "example": default,
                        "default": default,
                        "sensitive": name in {"git_conn_id", "artifact_root"},
                    }
                    for name, (kind, default, description) in _OPTIONS.items()
                },
            }
        },
    }
