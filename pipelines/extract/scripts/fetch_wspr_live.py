#!/usr/bin/env python3
"""WSPR.live — global amateur radio propagation spots, queried with raw SQL.

Sector: telecommunications / ionospheric physics. Nothing else in this
catalog is remotely like it, and I think it's the most unusual source in the
whole document.

**What it is:** thousands of amateur radio operators worldwide run very
low-power beacon transmitters and automated receivers. Every time a receiver
decodes a distant beacon it reports a "spot": who transmitted, who heard it,
on what frequency, at what signal-to-noise ratio, over what distance, at what
UTC minute. Millions of spots per day.

**Why it's a spectacular dataset:** each spot is a physical measurement of
the ionosphere. Radio propagation on HF depends on solar activity, time of
day, season, and geomagnetic storms — so the spot database is effectively a
distributed, crowd-funded, continuously-running sensor network for near-space
physics. You can watch the ionosphere open and close over the terminator each
day. Join it against the NOAA SWPC space weather feed already in this catalog
and you can try to predict propagation from solar wind conditions — a real
scientific question, answerable with two free APIs.

**The access method is unusual and delightful:** wspr.live exposes a public
**ClickHouse HTTP endpoint that accepts raw SQL** over the entire historical
spot database. No key, no auth, no pagination API to fight — you just write
SQL and choose an output format.

    http://db1.wspr.live/?query=<url-encoded SQL>

Documented example (from the wspr_cdk crate docs and wsprdaemon docs):
    SELECT * FROM wspr.rx LIMIT 5 FORMAT JSON

Verification status 2026-09-02: the host is **live and reachable** — it
answered my requests with HTTP 400 rather than failing to connect, meaning
the server is up and rejected my query text. I was NOT able to confirm the
column names from this sandbox. **Before trusting the SELECT lists below,
run `DESCRIBE TABLE wspr.rx FORMAT JSON` once and adjust.** The column names
used here follow the published sample record (id, time, band, rx_sign,
rx_lat, rx_lon, rx_loc, tx_sign, ...) but the rest are inferred.

Etiquette: this is a volunteer-run service holding billions of rows. Always
bound your queries with a time filter and a LIMIT, never `SELECT *` over the
full table, and keep polling gentle. Being rude here breaks a free community
resource for everyone.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "http://db1.wspr.live/"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def run_sql(sql: str):
    """Execute SQL against wspr.live. Returns parsed rows (FORMAT JSON)."""
    if "FORMAT" not in sql.upper():
        sql = sql.rstrip().rstrip(";") + " FORMAT JSON"
    url = BASE + "?" + urllib.parse.urlencode({"query": sql})
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=90) as resp:
        payload = json.load(resp)
    return payload.get("data", [])


def describe():
    """RUN THIS FIRST. Confirms the real schema before you trust any SELECT."""
    return run_sql("DESCRIBE TABLE wspr.rx")


def fetch_recent_spots(minutes: int = 10, limit: int = 5000,
                       band: int | None = None):
    """Spots decoded in the last N minutes. `band` is the WSPR band code."""
    where = [f"time > subtractMinutes(now(), {int(minutes)})"]
    if band is not None:
        where.append(f"band = {int(band)}")
    sql = (
        "SELECT time, band, rx_sign, rx_lat, rx_lon, rx_loc, "
        "tx_sign, tx_lat, tx_lon, tx_loc, distance, azimuth, snr, power, frequency "
        "FROM wspr.rx "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY time DESC LIMIT {int(limit)}"
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    for r in run_sql(sql):
        yield {
            "source": "wspr_live",
            "fetched_at": fetched_at,
            "id": f"{r.get('time')}|{r.get('tx_sign')}|{r.get('rx_sign')}|{r.get('band')}",
            "ts": r.get("time"),
            "band": r.get("band"),
            "frequency_hz": r.get("frequency"),
            "tx_call": r.get("tx_sign"),
            "tx_grid": r.get("tx_loc"),
            "tx_lat": r.get("tx_lat"),
            "tx_lon": r.get("tx_lon"),
            "rx_call": r.get("rx_sign"),
            "rx_grid": r.get("rx_loc"),
            "rx_lat": r.get("rx_lat"),
            "rx_lon": r.get("rx_lon"),
            "distance_km": r.get("distance"),
            "azimuth_deg": r.get("azimuth"),
            "snr_db": r.get("snr"),
            "tx_power_dbm": r.get("power"),
        }


def fetch_propagation_summary(minutes: int = 60):
    """Pre-aggregated: per band, how many spots and how far did signals reach?

    This is the cheap, polite poll — one small row per band instead of
    millions of raw spots, and it's the actual propagation signal you want
    for a time series.
    """
    sql = (
        "SELECT band, count() AS spots, "
        "round(avg(distance)) AS avg_km, max(distance) AS max_km, "
        "round(avg(snr), 1) AS avg_snr, uniq(tx_sign) AS unique_tx, "
        "uniq(rx_sign) AS unique_rx "
        "FROM wspr.rx "
        f"WHERE time > subtractMinutes(now(), {int(minutes)}) "
        "GROUP BY band ORDER BY band"
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    for r in run_sql(sql):
        yield {
            "source": "wspr_live_summary",
            "fetched_at": fetched_at,
            "id": f"{fetched_at}|{r.get('band')}",
            "window_minutes": minutes,
            "band": r.get("band"),
            "spots": r.get("spots"),
            "avg_distance_km": r.get("avg_km"),
            "max_distance_km": r.get("max_km"),
            "avg_snr_db": r.get("avg_snr"),
            "unique_transmitters": r.get("unique_tx"),
            "unique_receivers": r.get("unique_rx"),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "schema"
    if mode == "schema":
        print(json.dumps(describe(), indent=2))
    elif mode == "summary":
        for rec in fetch_propagation_summary():
            print(json.dumps(rec, ensure_ascii=False))
    else:
        for rec in fetch_recent_spots(minutes=5, limit=100):
            print(json.dumps(rec, ensure_ascii=False))
