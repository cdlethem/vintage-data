"""Load and validate the live-verified job-board catalog."""
from __future__ import annotations

import json
from pathlib import Path

from .common import Board, ISO2

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "catalogs" / "job_boards.json"
INDUSTRIES = {
    "Software", "Internet/Consumer Tech", "Semiconductors", "Hardware/Electronics",
    "Telecom", "Financial Services", "Banking", "Insurance", "Real Estate",
    "Healthcare Services", "Biotech/Pharma", "Medical Devices", "Retail",
    "Consumer Goods", "Food & Beverage", "Hospitality/Travel",
    "Transportation/Logistics", "Automotive", "Aerospace/Defense",
    "Industrial Manufacturing", "Energy/Utilities", "Mining/Materials", "Agriculture",
    "Construction/Engineering", "Professional Services", "Media/Entertainment",
    "Gaming", "Education", "Government/Public Sector", "Nonprofit/NGO", "Legal",
    "Marketing/Advertising", "Staffing/HR", "Security", "Crypto/Web3",
}


def load_catalog(path: Path = DEFAULT_PATH, providers: set[str] | None = None) -> list[Board]:
    document = json.loads(path.read_text(encoding="utf-8"))
    rows = document.get("boards") if isinstance(document, dict) else document
    if not isinstance(rows, list):
        raise ValueError(f"{path}: expected a list or an object with a boards list")

    boards: list[Board] = []
    seen: set[tuple[str, str]] = set()
    for index, row in enumerate(rows):
        missing = [key for key in ("provider", "token", "company", "country", "industry")
                   if not row.get(key)]
        if missing:
            raise ValueError(f"{path}: board {index} missing {missing}")
        if providers is not None and row["provider"] not in providers:
            continue
        if row["country"] not in ISO2:
            raise ValueError(f"{path}: board {index} has invalid country {row['country']!r}")
        if row["industry"] not in INDUSTRIES:
            raise ValueError(f"{path}: board {index} has invalid industry {row['industry']!r}")
        key = (row["provider"], row["token"])
        if key in seen:
            raise ValueError(f"{path}: duplicate provider/token {key}")
        seen.add(key)
        boards.append(Board(
            provider=row["provider"], token=row["token"], company=row["company"],
            country=row["country"], industry=row["industry"],
        ))
    return boards
