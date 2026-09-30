"""Registers the Bots page with Airflow (entry point ``airflow.plugins``).

Every Airflow component loads plugins, but only the API server serves pages, so the page
module and its web dependencies are imported on the first request rather than here.
"""
from airflow.plugins_manager import AirflowPlugin


async def _bots_page(scope, receive, send):
    from .web import app

    await app(scope, receive, send)


class BotsPlugin(AirflowPlugin):
    name = "airflow_bots"
    fastapi_apps = [{"app": _bots_page, "url_prefix": "/bots", "name": "Bots"}]
    external_views = [{"name": "Bots", "href": "/bots/", "destination": "nav", "url_route": "bots",
                       "nav_top_level": True}]
