"""Self-healing bots: the airflow-bots package, configured by $BOTS_CONFIG (example: orchestration/bots/config.example.yml)."""
from airflow_bots.dags import build

globals().update(build())
