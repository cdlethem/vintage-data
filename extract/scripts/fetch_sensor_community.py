#!/usr/bin/env python3
"""Sensor.Community — global citizen-run air quality and noise sensors.

Gap-list item: **citizen sensor networks.** Also one of the most genuinely
international sources in this catalog — a single request returns readings from
dozens of countries at once.

**What it is:** tens of thousands of DIY sensors, built and hosted by
volunteers, reporting particulate matter (PM1/PM2.5/PM10), temperature,
humidity, pressure, **noise levels**, and occasionally CO2. Grew out of the
German "Luftdaten" project in Stuttgart; the field names and some docs are
still German (`Feinstaub` = particulate matter, `DNMS (Laerm)` = the noise
sensor, `Laerm` = noise).

**Verified live 2026-09-03T00:41Z** — `data.sensor.community/static/v1/data.json`
returned readings timestamped 41 seconds before my request, from **28 countries
in a single payload**: DE, PL, NL, BG, HU, IT, CZ, CH, FR, GB, PT, ES, RO, RS,
SK, BA, AL, MK, AT, BE, LU, FI, RU, US, BR, ID, NP, AM.

**Why it's a good pipeline subject — and it's not the obvious reason.** The
data is *genuinely dirty*, in instructive ways you can see in the live sample:

  * `temperature: "-142.82"` and `"-136.61"` — BMP280 sensors failing and
    emitting garbage
  * `humidity: "99.90"` repeated across many sensors — a known DHT22 rail/stuck
    value, not real weather
  * `humidity: "1.00"` and `"0.00"` — the other failure rail
  * `value: "unavailable"` — a literal string where a float belongs
  * `indoor: 1` sensors mixed in with outdoor ones, which will wreck any
    spatial analysis if you don't filter
  * all values arrive as **strings**, not numbers

That combination makes this the best **data-quality engineering** exercise in
the whole catalog. Handling consumer hardware failure modes is a real skill and
most tutorial datasets are far too clean to teach it. The `quality_flags` field
below marks suspect readings rather than silently dropping them — you should
almost always keep the raw value and flag it, not discard it.

Analysis ideas: cross-border PM comparison (the sensor network doesn't respect
borders, so you can watch pollution advect between countries); urban noise
diurnal cycles; a sensor-reliability study (which hardware fails most, and
after how long); PM spikes at New Year from fireworks, which is one of the most
dramatic signals in any environmental dataset.

Endpoints (base `https://data.sensor.community`):
  /static/v1/data.json               ALL sensors, last 5 minutes  <- start here
  /airrohr/v1/filter/country=DE,PL   filter by country codes
  /airrohr/v1/filter/area=52.5,13.4,10   lat,lon,radius_km
  /airrohr/v1/filter/box=52.1,13.0,53.5,13.5
  /airrohr/v1/filter/type=SDS011,BME280
  /airrohr/v1/sensor/{id}/           one sensor's last 5 minutes

**The project explicitly requires a User-Agent** identifying you, so they can
contact you rather than block you if your requests become excessive (stated in
their API wiki, since 2022-11-04). Data refreshes every ~5 minutes; polling
faster is waste.

**Privacy note:** `exact_location: 1` means the volunteer published their
sensor's precise coordinates, often their home. `exact_location: 0` means it's
been rounded for privacy. Respect that distinction — don't un-blur, don't
aggregate in ways that re-identify a household, and prefer coarse spatial bins.

Stdlib only.
"""
import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone

BASE = "https://data.sensor.community"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"   # REQUIRED
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
MAX_SUMMARY_BYTES = 65_536
MAX_ERROR_CHARS = 1_000


class SensorCommunityFetchError(RuntimeError):
    def __init__(self, phase: str, cause: BaseException):
        self.phase = phase
        self.cause = cause
        super().__init__(str(cause))


def _safe_error(exc: BaseException) -> str:
    """Return a bounded, single-line diagnostic without common credentials."""
    detail = f"{type(exc).__name__}: {exc}"
    detail = re.sub(r"(?i)\b(bearer|basic)\s+[^\s,;]+", r"\1 [REDACTED]", detail)
    detail = re.sub(r"(?i)(authorization|api[_-]?key|token|password|secret)\s*[:=]\s*([^&\s,;]+)",
                    r"\1=[REDACTED]", detail)
    detail = re.sub(r"(https?://)[^/\s:@]+(?::[^/\s@]*)?@", r"\1[REDACTED]@", detail)
    return " ".join(detail.split())[:MAX_ERROR_CHARS] or "unspecified error"


def _emit_summary(payload: dict) -> None:
    line = SUMMARY_PREFIX + json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    if len((line + "\n").encode("utf-8")) >= MAX_SUMMARY_BYTES:
        raise AssertionError("run summary exceeds protocol size limit")
    print(line, file=sys.stderr)

# Observed failure rails in live data. Flag, don't silently drop.
SUSPECT = {
    "temperature": lambda v: v < -80 or v > 70,      # saw -142.82, -136.61
    "humidity":    lambda v: v <= 1.0 or v >= 99.85,  # DHT22 stuck rails
    "pressure":    lambda v: v < 30000 or v > 110000,
}
PM_TYPES = {"P0": "pm1", "P1": "pm10", "P2": "pm25", "P4": "pm4"}


def _get(path: str):
    req = urllib.request.Request(f"{BASE}/{path}",
                                 headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            try:
                return json.load(resp)
            except Exception as exc:
                raise SensorCommunityFetchError("decode", exc) from exc
    except SensorCommunityFetchError:
        raise
    except Exception as exc:
        raise SensorCommunityFetchError("request", exc) from exc


def _num(v):
    """Values arrive as strings, and sometimes as the literal 'unavailable'."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm(rec: dict, now: str):
    loc = rec.get("location") or {}
    sensor = rec.get("sensor") or {}
    stype = (sensor.get("sensor_type") or {})
    values, flags = {}, []

    for sv in rec.get("sensordatavalues") or []:
        vt = sv.get("value_type")
        raw = sv.get("value")
        num = _num(raw)
        if num is None:
            flags.append(f"nonnumeric:{vt}")        # e.g. "unavailable"
            continue
        key = PM_TYPES.get(vt, vt)
        values[key] = num
        check = SUSPECT.get(vt)
        if check and check(num):
            flags.append(f"suspect:{vt}={num}")

    indoor = bool(loc.get("indoor"))
    if indoor:
        flags.append("indoor")                       # exclude from spatial work

    return {
        "source": "sensor_community",
        "fetched_at": now,
        "id": rec.get("id"),
        "ts": rec.get("timestamp"),
        "location_id": loc.get("id"),
        "sensor_id": sensor.get("id"),
        "sensor_type": stype.get("name"),            # SDS011, BME280, DNMS...
        "manufacturer": stype.get("manufacturer"),
        "country": loc.get("country"),
        "latitude": _num(loc.get("latitude")),
        "longitude": _num(loc.get("longitude")),
        "altitude": _num(loc.get("altitude")),
        "indoor": indoor,
        "exact_location": bool(loc.get("exact_location")),  # privacy flag
        "values": values,          # pm25/pm10/temperature/humidity/noise_LAeq...
        "quality_flags": flags,    # empty list == looks clean
        "is_clean": not flags,
    }


def fetch_all(clean_only: bool = False, outdoor_only: bool = False):
    """Every sensor's last 5 minutes, worldwide. One request, ~28 countries."""
    now = datetime.now(timezone.utc).isoformat()
    for rec in _get("static/v1/data.json"):
        out = _norm(rec, now)
        if outdoor_only and out["indoor"]:
            continue
        if clean_only and not out["is_clean"]:
            continue
        yield out


def fetch_by_country(codes: str = "DE,PL,NL", **kw):
    """codes: comma-separated ISO country codes, e.g. 'ID,BR,NP'."""
    now = datetime.now(timezone.utc).isoformat()
    for rec in _get(f"airrohr/v1/filter/country={codes}"):
        yield _norm(rec, now)


def fetch_by_area(lat: float, lon: float, radius_km: float = 10):
    now = datetime.now(timezone.utc).isoformat()
    for rec in _get(f"airrohr/v1/filter/area={lat},{lon},{radius_km}"):
        yield _norm(rec, now)


def fetch_noise(**kw):
    """Only the DNMS noise sensors ('Laerm' = noise). Urban soundscape data."""
    for r in fetch_all(**kw):
        if any(k.startswith("noise") for k in r["values"]):
            yield r


def summarize(rows):
    """Quick QC report — run this before trusting anything."""
    rows = list(rows)
    countries, flagged, indoor = {}, 0, 0
    for r in rows:
        countries[r["country"]] = countries.get(r["country"], 0) + 1
        flagged += 0 if r["is_clean"] else 1
        indoor += 1 if r["indoor"] else 0
    return {"rows": len(rows), "countries": len(countries), "flagged": flagged,
            "indoor": indoor,
            "top_countries": sorted(countries.items(),
                                    key=lambda x: -x[1])[:10]}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="comma-separated country partition; production default remains global")
    parser.add_argument("--clean-only", action="store_true")
    parser.add_argument("--outdoor-only", action="store_true")
    args = parser.parse_args(argv)
    rows = fetch_by_country(args.country) if args.country else fetch_all(args.clean_only, args.outdoor_only)
    total = 0; countries = set(); sensors = set(); reading_ids = set(); indoors = exact = clean = 0; ts = []
    try:
        for row in rows:
            print(json.dumps(row, ensure_ascii=False)); total += 1
            countries.add(row["country"]); sensors.add(row["sensor_id"]); reading_ids.add(row["id"])
            indoors += bool(row["indoor"]); exact += bool(row["exact_location"]); clean += bool(row["is_clean"])
            if row["ts"]: ts.append(row["ts"])
    except Exception as exc:
        phase = exc.phase if isinstance(exc, SensorCommunityFetchError) else "iteration"
        cause = exc.cause if isinstance(exc, SensorCommunityFetchError) else exc
        error = _safe_error(cause)
        _emit_summary({
            "health": "failed", "completeness": "failed", "records": total,
            "error": error, "requests": {"attempted": 1},
            "partitions": {"attempted": 1, "succeeded": 0, "failed": 1},
            "metrics": {"failure_phase": phase},
        })
        return 1
    _emit_summary({
        "health": "healthy", "completeness": "complete", "records": total,
        "requests": {"attempted": 1}, "partitions": {"attempted": 1, "succeeded": 1, "failed": 0},
        "coverage": {"retrieval_mode": "country" if args.country else "global", "countries": len(countries),
                     "time_min": min(ts) if ts else None, "time_max": max(ts) if ts else None},
        "metrics": {"distinct_readings": len(reading_ids), "distinct_sensors": len(sensors),
                    "indoor": indoors, "exact_location": exact, "clean": clean}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
