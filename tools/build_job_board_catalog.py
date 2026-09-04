#!/usr/bin/env python3
"""Merge verified discovery outputs into the production job-board catalog.

Conflicting metadata is never settled by input order. Known ambiguous companies
have explicit resolutions below; any new conflict fails the build for review.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "extract" / "scripts"))
from job_boards_lib.adapters import FETCHERS  # noqa: E402
from job_boards_lib.catalog import INDUSTRIES  # noqa: E402
from job_boards_lib.common import ISO2  # noqa: E402

DISCOVERY = ROOT / "discovery"
OUTPUT = ROOT / "extract" / "catalogs" / "job_boards.json"

# provider/token -> canonical (company, HQ country, primary industry)
OVERRIDES = {
    ("ashby", "benchling"): ("Benchling", "US", "Biotech/Pharma"),
    ("greenhouse", "bitgo"): ("BitGo", "US", "Crypto/Web3"),
    ("greenhouse", "chime"): ("Chime", "US", "Financial Services"),
    ("greenhouse", "coinbase"): ("Coinbase", "US", "Crypto/Web3"),
    ("greenhouse", "earnin"): ("EarnIn", "US", "Financial Services"),
    ("greenhouse", "gusto"): ("Gusto", "US", "Staffing/HR"),
    ("greenhouse", "humaninterest"): ("Human Interest", "US", "Financial Services"),
    ("greenhouse", "sofi"): ("SoFi", "US", "Financial Services"),
    ("workday", "utaustin|wd1|UTstaff"): ("University of Texas at Austin", "US", "Education"),
    ("workday", "rollsroycesmr|wd103|rrsmr"): ("Rolls-Royce SMR", "GB", "Energy/Utilities"),
    ("workday", "thales|wd3|careers"): ("Thales", "FR", "Aerospace/Defense"),
    ("workday", "sanger|wd103|wellcomesangerinstitute"): ("Wellcome Sanger Institute", "GB", "Biotech/Pharma"),
    ("workday", "uoh|wd103|university_of_hull_careers"): ("University of Hull", "GB", "Education"),
    ("workday", "veoliauki|wd3|vescareers"): ("Veolia UK", "GB", "Energy/Utilities"),
    ("smartrecruiters", "Thales"): ("Thales", "FR", "Aerospace/Defense"),
    ("greenhouse", "canonical"): ("Canonical", "GB", "Software"),
    ("workday", "aveva|wd3|rib_careers"): ("AVEVA", "GB", "Software"),
    ("smartrecruiters", "RadiusLimited"): ("Radius Limited", "GB", "Telecom"),
    ("workday", "salesforce|wd12|External_Career_Site"): ("Salesforce", "US", "Software"),
    ("workday", "workday|wd5|Workday"): ("Workday", "US", "Software"),
    ("workday", "target|wd5|targetcareers"): ("Target", "US", "Retail"),
    ("greenhouse", "clear"): ("CLEAR", "US", "Security"),
    ("greenhouse", "chargepoint"): ("ChargePoint", "US", "Energy/Utilities"),
    ("greenhouse", "glossier"): ("Glossier", "US", "Consumer Goods"),
    ("workday", "merceruniversity|wd1|external"): ("Mercer University", "US", "Education"),
    ("workday", "rit|wd12|careers"): ("Rochester Institute of Technology", "US", "Education"),
    ("smartrecruiters", "BoschGroup"): ("Bosch Group", "DE", "Industrial Manufacturing"),
    ("greenhouse", "pinterest"): ("Pinterest", "US", "Internet/Consumer Tech"),
    ("greenhouse", "khanacademy"): ("Khan Academy", "US", "Education"),
    ("greenhouse", "cloudflare"): ("Cloudflare", "US", "Security"),
    ("greenhouse", "databricks"): ("Databricks", "US", "Software"),
    ("greenhouse", "discord"): ("Discord", "US", "Internet/Consumer Tech"),
    ("greenhouse", "elastic"): ("Elastic", "NL", "Software"),
    ("greenhouse", "reddit"): ("Reddit", "US", "Internet/Consumer Tech"),
    ("greenhouse", "collibra"): ("Collibra", "BE", "Software"),
    ("lever", "aleph"): ("Aleph", "AE", "Marketing/Advertising"),
    ("greenhouse", "carbon"): ("Carbon, Inc.", "US", "Industrial Manufacturing"),
    ("lever", "zeta"): ("Zeta", "IN", "Financial Services"),
    ("lever", "tsmg"): ("TSMG Holding", "PL", "Professional Services"),
    ("smartrecruiters", "EssilorLuxottica"): ("EssilorLuxottica", "FR", "Consumer Goods"),
    ("greenhouse", "algolia"): ("Algolia", "US", "Software"),
    ("lever", "swordhealth"): ("Sword Health", "US", "Healthcare Services"),
}


def main() -> int:
    paths = sorted(DISCOVERY.glob("verified_*.json"))
    if not paths:
        raise ValueError(f"no verified discovery files under {DISCOVERY}")

    grouped: dict[tuple[str, str], list[dict]] = {}
    input_rows = 0
    for path in paths:
        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError(f"{path}: expected a JSON list")
        for row in rows:
            input_rows += 1
            if row.get("status") != "ok" or not isinstance(row.get("open_postings"), int) \
                    or row["open_postings"] <= 0:
                raise ValueError(f"{path}: unverified row {row!r}")
            if row.get("provider") not in FETCHERS:
                raise ValueError(f"{path}: unsupported provider {row.get('provider')!r}")
            if row.get("country") not in ISO2 or row.get("industry") not in INDUSTRIES:
                raise ValueError(f"{path}: invalid metadata {row!r}")
            key = (row["provider"], row["token"])
            grouped.setdefault(key, []).append(row)

    catalog = []
    unresolved = []
    for key, rows in grouped.items():
        variants = {(r["company"], r["country"], r["industry"]) for r in rows}
        if len(variants) == 1:
            company, country, industry = variants.pop()
        elif key in OVERRIDES:
            company, country, industry = OVERRIDES[key]
        else:
            unresolved.append((key, sorted(variants)))
            continue
        catalog.append({
            "provider": key[0], "token": key[1], "company": company,
            "country": country, "industry": industry,
            "verified_open_postings": max(r["open_postings"] for r in rows),
        })

    if unresolved:
        detail = "\n".join(f"  {key}: {variants}" for key, variants in unresolved)
        raise ValueError(f"unresolved catalog metadata conflicts:\n{detail}")

    catalog.sort(key=lambda row: (row["provider"], row["company"].casefold(), row["token"]))
    document = {
        "verified_at": date.today().isoformat(),
        "source_files": [path.name for path in paths],
        "boards": catalog,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    by_provider = Counter(row["provider"] for row in catalog)
    by_country = Counter(row["country"] for row in catalog)
    by_industry = Counter(row["industry"] for row in catalog)
    postings = sum(row["verified_open_postings"] for row in catalog)
    print(f"inputs={input_rows} unique_boards={len(catalog)} verified_postings={postings}")
    print(f"providers={dict(sorted(by_provider.items()))}")
    print(f"countries={len(by_country)} industries={len(by_industry)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
