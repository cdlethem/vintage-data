"""Self-healing bots: the airflow-bots package, configured by $BOTS_CONFIG (orchestration/bots/config.yml)."""
from airflow_bots.dags import build

globals().update(build())
