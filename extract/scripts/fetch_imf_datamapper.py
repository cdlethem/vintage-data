#!/usr/bin/env python3
"""Fetch IMF DataMapper real GDP growth observations as NDJSON.

The extractor intentionally supports only the official ``NGDP_RPCH`` endpoint.
DataMapper includes regional aggregates beside economies, so only known economy
codes are emitted. Years and numeric values are retained exactly as decoded from
the API; no actual/projected classification is inferred.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
import re
import sys
from typing import Any, Iterator, Sequence
import urllib.error
import urllib.request

SOURCE = "imf_datamapper"
INDICATOR = "NGDP_RPCH"
API_URL = "https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH"
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 120
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
YEAR_RE = re.compile(r"[0-9]{4}")
ENTITY_RE = re.compile(r"[A-Z0-9_]+")
MAX_DIAGNOSTIC_CHARS = 1000
_URL_CREDENTIALS_RE = re.compile(
    r"(?i)\b(https?://)[^/\s:@]+(?::[^/\s@]*)?@"
)
_SECRET_RE = re.compile(
    r"(?i)\b(authorization|proxy-authorization|api[-_]?key|access[-_]?token|"
    r"token|password|secret)\s*([:=])\s*(?:bearer\s+|basic\s+)?[^\s&]+"
)

# DataMapper mixes economies and aggregates in the same value map. Emit only
# known ISO alpha-3 economies (plus IMF-specific Kosovo and West Bank/Gaza
# codes); an unknown code is conservatively omitted rather than mislabeled as
# a country.
COUNTRY_CODES = frozenset(
    """
    ABW AFG AGO AIA ALA ALB AND ARE ARG ARM ASM ATA ATF ATG AUS AUT AZE
    BDI BEL BEN BES BFA BGD BGR BHR BHS BIH BLM BLR BLZ BMU BOL BRA BRB
    BRN BTN BVT BWA CAF CAN CCK CHE CHL CHN CIV CMR COD COG COK COL COM
    CPV CRI CUB CUW CXR CYM CYP CZE DEU DJI DMA DNK DOM DZA ECU EGY ERI
    ESH ESP EST ETH FIN FJI FLK FRA FRO FSM GAB GBR GEO GGY GHA GIB GIN
    GLP GMB GNB GNQ GRC GRD GRL GTM GUF GUM GUY HKG HMD HND HRV HTI HUN
    IDN IMN IND IOT IRL IRN IRQ ISL ISR ITA JAM JEY JOR JPN KAZ KEN KGZ
    KHM KIR KNA KOR KWT LAO LBN LBR LBY LCA LIE LKA LSO LTU LUX LVA MAC
    MAF MAR MCO MDA MDG MDV MEX MHL MKD MLI MLT MMR MNE MNG MNP MOZ MRT
    MSR MTQ MUS MWI MYS MYT NAM NCL NER NFK NGA NIC NIU NLD NOR NPL NRU
    NZL OMN PAK PAN PCN PER PHL PLW PNG POL PRI PRK PRT PRY PSE PYF QAT
    REU ROU RUS RWA SAU SDN SEN SGP SGS SHN SJM SLB SLE SLV SMR SOM SPM
    SRB SSD STP SUR SVK SVN SWE SWZ SXM SYC SYR TCA TCD TGO THA TJK TKL
    TKM TLS TON TTO TUN TUR TUV TWN TZA UGA UKR UMI URY USA UZB VAT VCT
    VEN VGB VIR VNM VUT WBG WLF WSM YEM ZAF ZMB ZWE UVK ANT SCG
    """.split()
)


class IMFDataMapperError(RuntimeError):
    """The DataMapper request or response cannot satisfy the extract contract."""


def _safe_diagnostic_detail(detail: object, limit: int = MAX_DIAGNOSTIC_CHARS) -> str:
    """Return a single-line, bounded diagnostic without common credential forms."""
    text = str(detail)
    text = " ".join(
        "".join(character if character.isprintable() else " " for character in text).split()
    )
    text = _URL_CREDENTIALS_RE.sub(r"\1[redacted]@", text)
    text = _SECRET_RE.sub(r"\1\2[redacted]", text)
    if not text:
        text = type(detail).__name__
    if len(text) > limit:
        text = f"{text[: limit - 3]}..."
    return text




def _validate_request(indicator: str, timeout: int) -> None:
    if indicator != INDICATOR:
        raise ValueError(f"indicator must be {INDICATOR}")
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= MAX_TIMEOUT:
        raise ValueError(f"timeout must be an integer between 1 and {MAX_TIMEOUT}")


def stable_observation_id(indicator: str, entity_code: str, year: str) -> str:
    """Return an identity independent of fetch time and the observed value."""
    canonical = json.dumps(
        [indicator, entity_code, year], ensure_ascii=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def parse_response(document: Any, indicator: str, fetched_at: str) -> list[dict[str, Any]]:
    """Validate a complete DataMapper payload before returning observations."""
    if not isinstance(document, dict):
        raise IMFDataMapperError("IMF DataMapper payload must be an object")
    values = document.get("values")
    if not isinstance(values, dict):
        raise IMFDataMapperError("IMF DataMapper payload has invalid values object")
    series_by_entity = values.get(indicator)
    if not isinstance(series_by_entity, dict):
        raise IMFDataMapperError(
            f"IMF DataMapper payload has invalid {indicator} indicator object"
        )
    if not series_by_entity:
        raise IMFDataMapperError(f"IMF DataMapper {indicator} payload is empty")

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for entity_code in sorted(series_by_entity):
        if not isinstance(entity_code, str) or ENTITY_RE.fullmatch(entity_code) is None:
            raise IMFDataMapperError("IMF DataMapper payload has an invalid entity code")
        series = series_by_entity[entity_code]
        if not isinstance(series, dict) or not series:
            raise IMFDataMapperError(
                f"IMF DataMapper entity {entity_code} has no observations"
            )
        is_country = entity_code in COUNTRY_CODES
        for year in sorted(series):
            if not isinstance(year, str) or YEAR_RE.fullmatch(year) is None:
                raise IMFDataMapperError(
                    f"IMF DataMapper entity {entity_code} has invalid year {year!r}"
                )
            value = series[year]
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise IMFDataMapperError(
                    f"IMF DataMapper {entity_code}/{year} has invalid observation value"
                )
            if not is_country:
                continue
            observation_id = stable_observation_id(indicator, entity_code, year)
            if observation_id in seen_ids:
                raise IMFDataMapperError(
                    f"IMF DataMapper payload contains duplicate observation {entity_code}/{year}"
                )
            seen_ids.add(observation_id)
            records.append(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": observation_id,
                    "indicator": indicator,
                    "country": entity_code,
                    "year": year,
                    "value": value,
                }
            )

    if not records:
        raise IMFDataMapperError(
            f"IMF DataMapper {indicator} payload contains no country observations"
        )
    return records


def _read_document(response: Any) -> Any:
    status = response.getcode()
    if status is not None and not 200 <= status < 300:
        raise IMFDataMapperError(f"IMF DataMapper returned HTTP {status}")
    payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise IMFDataMapperError(
            f"IMF DataMapper response exceeds {MAX_RESPONSE_BYTES} bytes"
        )
    try:
        return json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise IMFDataMapperError("IMF DataMapper returned invalid JSON") from exc


def fetch_imf_datamapper(
    indicator: str = INDICATOR, timeout: int = DEFAULT_TIMEOUT
) -> Iterator[dict[str, Any]]:
    """Fetch and validate the one supported IMF DataMapper indicator."""
    _validate_request(indicator, timeout)
    request = urllib.request.Request(
        API_URL,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = _read_document(response)
    except urllib.error.HTTPError as exc:
        raise IMFDataMapperError(f"IMF DataMapper returned HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        detail = _safe_diagnostic_detail(exc.reason)
        raise IMFDataMapperError(f"IMF DataMapper network failure: {detail}") from exc
    except http.client.IncompleteRead as exc:
        raise IMFDataMapperError(
            "IMF DataMapper network/protocol failure: incomplete response"
        ) from exc
    except http.client.HTTPException as exc:
        raise IMFDataMapperError(
            f"IMF DataMapper network/protocol failure: {type(exc).__name__}"
        ) from exc
    except (TimeoutError, OSError) as exc:
        detail = _safe_diagnostic_detail(exc)
        raise IMFDataMapperError(f"IMF DataMapper network failure: {detail}") from exc

    fetched_at = datetime.now(timezone.utc).isoformat()
    yield from parse_response(document, indicator, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch IMF DataMapper real GDP growth observations as NDJSON."
    )
    parser.add_argument(
        "indicator",
        nargs="?",
        default=INDICATOR,
        choices=(INDICATOR,),
        help=f"DataMapper indicator (only {INDICATOR} is supported)",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    try:
        for record in fetch_imf_datamapper(args.indicator, args.timeout):
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except (IMFDataMapperError, ValueError) as exc:
        parser.exit(1, f"fetch_imf_datamapper: {exc}\n")


if __name__ == "__main__":
    main()
