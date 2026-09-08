"""Airflow plugin registration; import performs config and packaged-file reads only."""
from __future__ import annotations

import importlib
import importlib.resources
import json
import logging
from urllib.parse import urljoin, urlsplit
from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


def _enabled(name: str, fallback: bool = False) -> bool:
    return conf.getboolean("bot_dashboard", name, fallback=fallback)


def _react_apps() -> list[dict]:
    if not _enabled("enabled"): return []
    try:
        base_url = conf.get("api", "base_url")
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("[api] base_url must be a canonical HTTP(S) URL")
        root = importlib.resources.files("airflow.providers.vintage.bot_dashboard").joinpath("static")
        manifest = json.loads(root.joinpath("asset-manifest.json").read_text("utf-8"))
        dashboard = manifest["dashboard"]
        application = manifest["app"]
        for asset in (dashboard, application):
            if not isinstance(asset, str) or not asset.startswith(("dashboard/main.", "app/main.")) or not asset.endswith(".umd.cjs") or not root.joinpath(asset).is_file():
                raise ValueError("asset manifest references an invalid bundle")
        prefix = parsed.path.rstrip("/") + "/"
        apps = [{"name": "Bot Activity", "url_route": "bot-activity", "destination": "nav", "nav_top_level": True, "bundle_url": urljoin(prefix, f"bot-dashboard/static/{application}")}]
        if _enabled("widget_enabled"):
            apps.insert(0, {"name": "Bot action queue", "url_route": "bot-actions", "destination": "dashboard", "bundle_url": urljoin(prefix, f"bot-dashboard/static/{dashboard}")})
        return apps
    except Exception as exc:
        log.error("bot dashboard React registration disabled: %s", exc)
        return []


class VintageBotDashboardPlugin(AirflowPlugin):
    name = "vintage_data_bot_dashboard"
    fastapi_apps = (
        [
            {
                "app": importlib.import_module(
                    "airflow.providers.vintage.bot_dashboard.api"
                ).app,
                "url_prefix": "/bot-dashboard",
                "name": "Vintage Bot Dashboard API",
            }
        ]
        if _enabled("enabled")
        else []
    )
    react_apps = _react_apps()
