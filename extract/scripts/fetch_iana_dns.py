#!/usr/bin/env python3
"""IANA/InterNIC — DNS root-zone and RDAP bootstrap snapshots.

Root records expose delegation and DNSSEC changes; the RDAP bootstrap maps TLDs to
registration services. Both official files were verified live 2026-09-03. Root-zone RR
identity includes normalized RDATA, so replacements appear as removal/addition across
snapshots. The zone is roughly 2.2 MB and the bootstrap roughly 71 KB. Poll daily and
follow IANA's file-use terms.

Stdlib only.
"""

import argparse
import hashlib
import json
import os
import shlex
import urllib.request
from datetime import datetime, timezone

ROOT_URL = "https://www.internic.net/domain/root.zone"
RDAP_URL = "https://data.iana.org/rdap/dns.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(url: str, timeout: int):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(), response.headers.get("Last-Modified")


def fetch_root_zone(limit: int | None = None, timeout: int = 60):
    if limit is not None and limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    body, modified = _get(ROOT_URL, timeout)
    statements, current, balance = [], "", 0
    for physical in body.decode("utf-8", "replace").splitlines():
        content = physical.split(";", 1)[0].rstrip()
        if not content.strip():
            continue
        current = f"{current} {content.strip()}".strip()
        balance += content.count("(") - content.count(")")
        if balance == 0:
            statements.append(
                (physical[:1].isspace(), current.replace("(", "").replace(")", ""))
            )
            current = ""
    owner = None
    for emitted, (inherited, statement) in enumerate(statements, 1):
        tokens = shlex.split(statement)
        if inherited:
            if owner is None:
                raise ValueError("root-zone record inherits an unknown owner")
        else:
            owner, tokens = tokens[0], tokens[1:]
        if len(tokens) < 4:
            raise ValueError(f"unrecognized root-zone record: {statement[:120]}")
        ttl, dns_class, record_type = tokens[:3]
        rdata = " ".join(tokens[3:])
        identity = f"{owner}|{record_type}|{rdata}"
        yield {
            "source": "iana_root_zone",
            "fetched_at": fetched_at,
            "id": hashlib.sha256(identity.encode()).hexdigest(),
            "owner": owner,
            "ttl": int(ttl),
            "class": dns_class,
            "type": record_type,
            "rdata": rdata,
            "publisher_updated_at": modified,
        }
        if limit is not None and emitted >= limit:
            return


def fetch_rdap(limit: int | None = None, timeout: int = 30):
    if limit is not None and limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    body, modified = _get(RDAP_URL, timeout)
    document = json.loads(body)
    services = document.get("services")
    if not isinstance(services, list):
        raise TypeError("RDAP bootstrap is missing services")
    for index, service in enumerate(services):
        if limit is not None and index >= limit:
            break
        if not isinstance(service, list) or len(service) != 2:
            raise ValueError("RDAP service has an unexpected shape")
        tlds, urls = service
        identity = "|".join(sorted(tlds))
        yield {
            "source": "iana_rdap_bootstrap",
            "fetched_at": fetched_at,
            "id": hashlib.sha256(identity.encode()).hexdigest(),
            "tlds": tlds,
            "urls": urls,
            "publication": document.get("publication"),
            "publisher_updated_at": modified,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("root_zone", "rdap"), default="root_zone")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    records = (
        fetch_rdap(args.limit, args.timeout)
        if args.dataset == "rdap"
        else fetch_root_zone(args.limit, args.timeout)
    )
    for record in records:
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
