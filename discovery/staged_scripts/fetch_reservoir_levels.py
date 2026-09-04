#!/usr/bin/env python3
"""California CDEC — reservoir storage, inflow, outflow. Drought as a series.

Gap-list item: **reservoir levels.**

**What it is:** the California Data Exchange Center, run by the Department of
Water Resources, publishes sensor readings for every major reservoir, river
gauge and snow course in the state as keyless JSON. Storage, elevation,
inflow, outflow and precipitation, at hourly, daily and monthly resolution,
decades deep.

**Verified live 2026-09-03:**
  * `JSONDataServlet?Stations=SHA&SensorNums=15&dur_code=D` → Shasta daily
    storage; on 2026-08-25 it held **2,690,849 acre-feet** and fell to
    2,669,273 AF three days later — visible drawdown, day over day.
  * A five-station, one-year pull returned **1,835 rows**, of which **10 were
    the sentinel `-9999`** and **17 carried revision flags**.
  * Multi-sensor works in one request: sensors 15/6/76/23/45 returned
    STORAGE (AF), RES ELE (FEET), INFLOW (CFS), OUTFLOW (CFS), PPT INC (INCHES).
  * `dur_code=H` returns hourly rows — Shasta's water surface elevation moved
    990.63 → 990.64 ft between two consecutive hours.

**Why it's a good pipeline subject.** Most "slow" environmental data is boring
because nothing happens between polls. Reservoirs are different: storage is a
**physical accumulator**, and the API gives you its derivative too. Inflow and
outflow are in CFS, storage is in acre-feet, and

    Δstorage ≈ (inflow − outflow) × Δt

so you can **check the data against itself.** A pipeline that reconciles the
integral against the differences catches sensor faults, missing days, and unit
errors without any external ground truth. That is a genuinely useful exercise
and almost no tutorial dataset supports it.

Beyond that:
  * **Percent-of-capacity is the human-legible metric** and it is seasonal,
    multi-year, and currently consequential. Drought is one of the few slow
    signals where a five-year series actually tells a story.
  * **Snowpack leads storage by months.** CDEC also publishes snow water
    equivalent; the snowpack→runoff→storage chain is a natural lagged-predictor
    problem with all three legs on the same free API.
  * **Revisions are flagged.** `dataFlag` of `e` (estimated) or `r` (revised)
    marks values that were corrected after the fact — so, like UK Carbon
    Intensity (#14) and NVD (#16), re-polling the past is not wasted work.

Quirks that will cost you an afternoon:

  * **`-9999` is the missing-data sentinel, and it is a plain integer.** It
    sits in the same field as real values and will not announce itself. Ten of
    1,835 rows in my sample. Averaging without filtering it turns a full
    reservoir into a negative number; `_value()` maps it to `None`.
  * **Today's daily row already exists, holding `-9999`.** CDEC emits the row
    for the current day as soon as the day starts and backfills the value once
    it closes. So "take the most recent row" returns the sentinel, not the
    current level — confirmed live on 2026-09-02. Take the most recent row
    *with a non-missing value*, which is what `storage_report()` does.
  * **Dates are NOT zero-padded**: `"2026-8-25 00:00"`, and hourly rows read
    `"2026-9-1 0:00"`. They sort wrong lexically and break strict ISO parsers.
    `parse_cdec_date()` normalises to real ISO.
  * **`date` and `obsDate` differ by one interval** on daily records — the
    daily value stamped `2026-8-25` carries `obsDate` `2026-8-26`, because it
    describes the period *ending* at that instant. Pick one deliberately and
    write down which; the script keeps both.
  * **`dataFlag` is a single space `' '` when normal**, not empty and not null.
    Truthiness tests on it are wrong; strip first.
  * **Capacity is not in the API.** Percent-of-capacity needs a constant per
    reservoir, so a small table is embedded below — every entry checked against
    six years of observed maxima (see `validate_capacities()`).
  * **Percent-of-capacity legitimately exceeds 100%.** Berryessa hit 100.4% and
    Millerton 101.6% in the validation run, because reservoirs are operated
    into flood storage above nominal capacity. Do not clamp it; a clamp hides
    exactly the flood-operation events you would most want to see.
  * The servlet is tolerant of bad station ids — it returns fewer rows rather
    than erroring, so a typo silently yields nothing for that station.

**Etiquette.** This is a state agency's operational system, not a CDN. Daily
data changes once a day and hourly data once an hour, so **poll daily**; pull
long history once and cache it, because past days are stable apart from the
occasional `r` revision. Batch stations and sensors into one request — the
servlet accepts comma-separated lists and it is far cheaper for them than N
requests. Data is public domain as a work of California state government;
DWR asks for attribution.

Endpoints:
  https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet
      ?Stations=SHA,ORO&SensorNums=15,76,23&dur_code=D&Start=YYYY-MM-DD&End=YYYY-MM-DD
  dur_code: H (hourly) | D (daily) | M (monthly)

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

BASE = "https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

MISSING = -9999          # sentinel sitting in the same field as real values

SENSORS = {
    15: ("storage", "AF"),        # reservoir storage, acre-feet
    6:  ("elevation", "FEET"),    # water surface elevation
    76: ("inflow", "CFS"),        # cubic feet per second
    23: ("outflow", "CFS"),
    45: ("precip_incremental", "INCHES"),
    3:  ("snow_water_equivalent", "INCHES"),
}

# Gross pool capacity in acre-feet. Every figure below was checked against the
# station's observed maximum over 2020-01-01..2026-09-02 and lands in
# 87-102% of it — see validate_capacities(). Stations whose published capacity
# did not reconcile are deliberately omitted rather than guessed at.
CAPACITY_AF = {
    "SHA": 4552000,   # Shasta
    "ORO": 3537577,   # Oroville
    "CLE": 2447650,   # Trinity
    "NML": 2400000,   # New Melones
    "SNL": 2041000,   # San Luis
    "DNP": 2030000,   # Don Pedro
    "BER": 1602000,   # Berryessa
    "EXC": 1024600,   # McClure
    "PNF": 1000000,   # Pine Flat
    "FOL": 977000,    # Folsom
    "BUL": 966103,    # New Bullards Bar
    "ISB": 568000,    # Isabella
    "MIL": 520500,    # Millerton
    "CMN": 417120,    # Camanche
}
MAJOR = list(CAPACITY_AF)


def _get(stations, sensors, dur_code, start, end):
    params = {
        "Stations": ",".join(stations) if not isinstance(stations, str)
                    else stations,
        "SensorNums": ",".join(str(s) for s in sensors)
                      if not isinstance(sensors, str) else sensors,
        "dur_code": dur_code,
        "Start": start,
        "End": end,
    }
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


def parse_cdec_date(s):
    """'2026-8-25 00:00' and '2026-9-1 0:00' -> ISO. Nothing is zero-padded."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M").isoformat()
    except ValueError:
        return None


def _value(v):
    """-9999 is missing, not a reading."""
    if v is None or v == MISSING:
        return None
    return v


def fetch(stations=None, sensors=(15, 76, 23), dur_code="D",
          start=None, end=None):
    """Reservoir readings, normalised. Batch stations/sensors into one call."""
    stations = list(stations or MAJOR)
    end = end or date.today().isoformat()
    start = start or (date.today() - timedelta(days=30)).isoformat()
    now = datetime.now(timezone.utc).isoformat()

    for r in _get(stations, sensors, dur_code, start, end):
        station = r.get("stationId")
        sensor_num = r.get("SENSOR_NUM")
        name, _unit = SENSORS.get(sensor_num, (None, None))
        value = _value(r.get("value"))
        capacity = CAPACITY_AF.get(station)
        # only meaningful for storage, and legitimately can exceed 100%
        pct = (round(100.0 * value / capacity, 3)
               if value is not None and capacity and sensor_num == 15
               else None)
        flag = (r.get("dataFlag") or "").strip()      # ' ' when normal
        obs = parse_cdec_date(r.get("date"))
        yield {
            "source": "cdec_reservoir",
            "fetched_at": now,
            "id": f"{station}:{sensor_num}:{dur_code}:{obs}",
            "station": station,
            "sensor_num": sensor_num,
            "measure": name or (r.get("sensorType") or "").lower().strip(),
            "sensor_type": r.get("sensorType"),
            "dur_code": r.get("durCode"),
            # period start; obs_end is the same instant + one interval
            "obs_date": obs,
            "obs_end": parse_cdec_date(r.get("obsDate")),
            "value": value,                  # None when the sentinel was seen
            "units": r.get("units"),
            "is_missing": r.get("value") == MISSING,
            "data_flag": flag or None,       # 'e' estimated, 'r' revised
            "is_estimated": flag == "e",
            "is_revised": flag == "r",
            "capacity_af": capacity if sensor_num == 15 else None,
            "pct_of_capacity": pct,
        }


def storage_report(stations=None, day=None):
    """Current storage and fill percentage for the major reservoirs."""
    day = day or date.today().isoformat()
    start = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    latest = {}
    for r in fetch(stations, sensors=(15,), dur_code="D",
                   start=start, end=day):
        if r["value"] is None:
            continue
        cur = latest.get(r["station"])
        if cur is None or (r["obs_date"] or "") > (cur["obs_date"] or ""):
            latest[r["station"]] = r
    return sorted(latest.values(),
                  key=lambda x: -(x["pct_of_capacity"] or 0))


def water_balance(station: str = "SHA", start=None, end=None):
    """**The self-consistency check.** Compare observed day-over-day storage
    change against (inflow - outflow) integrated over the day.

    1 CFS for 24h = 86400 ft^3 = 1.98347 acre-feet. A large, persistent
    residual means a bad sensor, an ungauged diversion, evaporation, or a unit
    error in your own code — all worth catching, none visible in storage alone.
    """
    end = end or date.today().isoformat()
    start = start or (date.fromisoformat(end) - timedelta(days=30)).isoformat()
    CFS_DAY_TO_AF = 86400.0 / 43560.0        # 1.98347

    series = {}
    for r in fetch([station], sensors=(15, 76, 23), dur_code="D",
                   start=start, end=end):
        if r["value"] is None or not r["obs_date"]:
            continue
        series.setdefault(r["obs_date"][:10], {})[r["measure"]] = r["value"]

    now = datetime.now(timezone.utc).isoformat()
    days = sorted(series)
    for prev, cur in zip(days, days[1:]):
        a, b = series[prev], series[cur]
        if "storage" not in a or "storage" not in b:
            continue
        observed = b["storage"] - a["storage"]
        inflow, outflow = b.get("inflow"), b.get("outflow")
        expected = ((inflow - outflow) * CFS_DAY_TO_AF
                    if inflow is not None and outflow is not None else None)
        yield {
            "source": "cdec_water_balance",
            "fetched_at": now,
            "id": f"{station}:{cur}",
            "station": station,
            "date": cur,
            "storage_af": b["storage"],
            "delta_storage_af": round(observed, 1),
            "inflow_cfs": inflow,
            "outflow_cfs": outflow,
            "expected_delta_af": round(expected, 1)
                                 if expected is not None else None,
            "residual_af": round(observed - expected, 1)
                           if expected is not None else None,
        }


def validate_capacities(years: int = 6):
    """Re-check the embedded capacity table against observed maxima.

    Run this if a percent-of-capacity number ever looks wrong. Anything far
    outside ~85-105% means the constant is stale or the station id moved.
    """
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=365 * years)).isoformat()
    peak = {}
    for r in fetch(MAJOR, sensors=(15,), dur_code="D", start=start, end=end):
        if r["value"] is not None:
            peak[r["station"]] = max(peak.get(r["station"], 0), r["value"])
    now = datetime.now(timezone.utc).isoformat()
    for st, cap in sorted(CAPACITY_AF.items()):
        obs = peak.get(st)
        yield {
            "source": "cdec_capacity_check",
            "fetched_at": now,
            "id": st,
            "station": st,
            "capacity_af": cap,
            "observed_max_af": obs,
            "observed_pct": round(100.0 * obs / cap, 1) if obs else None,
            "plausible": bool(obs and 0.5 <= obs / cap <= 1.10),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "report"
    if mode == "report":
        for r in storage_report():
            print(json.dumps({k: r[k] for k in
                              ("station", "obs_date", "value", "units",
                               "capacity_af", "pct_of_capacity")}))
    elif mode == "balance":
        for r in water_balance(sys.argv[2] if len(sys.argv) > 2 else "SHA"):
            print(json.dumps(r))
    elif mode == "validate":
        for r in validate_capacities():
            print(json.dumps(r))
    else:
        for r in fetch():
            print(json.dumps(r))
