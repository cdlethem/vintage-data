#!/usr/bin/env python3
"""IMDb non-commercial datasets — daily title rating changes.

Vote counts and average ratings move continuously, making title-level rating trajectories
a useful popularity signal. Verified live 2026-09-03 against the official gzipped
``title.ratings.tsv.gz`` file. IMDb permits these datasets only for personal and
non-commercial use. Refresh daily.

IMDb publishes only a complete daily dump — there is no changes endpoint — so this
narrows the output in the two places the publisher does support:

1. A conditional request. The publisher serves ``Last-Modified``, so the stored value is
   sent back as ``If-Modified-Since``; an unchanged dump answers ``304`` and the run ends
   with no records at all, without downloading ~1.7M rows.
2. A diff against the previous dump. Only titles whose ``averageRating`` or ``numVotes``
   actually moved (plus titles new to the dataset) are emitted, each carrying its previous
   values so the change is measurable directly. The full file is still streamed - that is
   unavoidable with a dump-only publisher - but it is no longer re-emitted verbatim every
   day, which was ~1.7M near-identical records per run.

The first run has no baseline and therefore emits the whole dataset once, which is the
backfill. ``--full`` forces that behaviour again, and ``--no-state`` restores the old
stateless full-dump output. State is written only after every record is printed, so an
aborted run re-fetches rather than skipping (at-least-once).

Stdlib only.
"""

import argparse
import csv
import gzip
import io
import json
import os
import pathlib
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "imdb_title_ratings"
URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_HEADER = "# imdb_title_ratings state v1"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the previous dump's ratings live: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.tsv.gz"


def load_state(path: pathlib.Path):
    """Return (last_modified, {tconst: (rating, votes)}). Missing state is a cold start."""
    previous: dict[str, tuple[str, str]] = {}
    try:
        handle = gzip.open(path, "rt", encoding="utf-8")
    except FileNotFoundError:
        return "", previous
    with handle:
        first = handle.readline().rstrip("\n")
        if not first.startswith(STATE_HEADER):
            raise ValueError(f"malformed rating state in {path}")
        last_modified = first[len(STATE_HEADER):].strip()
        for line in handle:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 3:
                raise ValueError(f"malformed rating state row in {path}: {line[:80]!r}")
            previous[parts[0]] = (parts[1], parts[2])
    return last_modified, previous


def save_state(path: pathlib.Path, last_modified: str, current: dict[str, tuple[str, str]]):
    """Publish the state atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    with gzip.open(staged, "wt", encoding="utf-8") as handle:
        handle.write(f"{STATE_HEADER} {last_modified}\n")
        for title_id, (rating, votes) in current.items():
            handle.write(f"{title_id}\t{rating}\t{votes}\n")
    os.replace(staged, path)


def fetch_ratings(limit: int | None = None, timeout: int = 120, previous=None,
                  last_modified: str = "", current=None):
    """Yield changed rating records, recording every row in ``current`` for the next run.

    ``previous`` is the prior dump keyed by tconst; ``None`` means emit everything.
    """
    if limit is not None and limit <= 0:
        return
    headers = {"User-Agent": USER_AGENT}
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    request = urllib.request.Request(URL, headers=headers)
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if error.code == 304:
            return  # publisher's dump is unchanged: nothing to measure this run
        raise
    with response:
        modified = response.headers.get("Last-Modified")
        with gzip.GzipFile(fileobj=response) as compressed:
            reader = csv.DictReader(
                io.TextIOWrapper(compressed, encoding="utf-8"), delimiter="\t"
            )
            if not reader.fieldnames or "tconst" not in reader.fieldnames:
                raise ValueError("IMDb ratings TSV is missing tconst")
            for index, row in enumerate(reader):
                if limit is not None and index >= limit:
                    break
                title_id = row.get("tconst")
                if not title_id:
                    raise ValueError("IMDb rating is missing tconst")
                rating = row.get("averageRating") or ""
                votes = row.get("numVotes") or ""
                if current is not None:
                    current[title_id] = (rating, votes)
                if previous is not None:
                    was = previous.get(title_id)
                    if was == (rating, votes):
                        continue  # unchanged since the last dump
                record = dict(row)
                record.update(
                    {
                        "source": SOURCE,
                        "fetched_at": fetched_at,
                        "id": title_id,
                        "publisher_updated_at": modified,
                    }
                )
                if previous is not None:
                    was = previous.get(title_id)
                    record["change"] = "new" if was is None else "updated"
                    record["previous_average_rating"] = was[0] if was else None
                    record["previous_num_votes"] = was[1] if was else None
                yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="cap rows scanned; for smoke tests")
    parser.add_argument("--full", action="store_true",
                        help="emit every title, not just changes (re-baseline)")
    parser.add_argument("--state-file", help="state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="stateless full-dump output; do not read or write state")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    path = state_path(args.state_file)
    if args.no_state:
        for record in fetch_ratings(args.limit, args.timeout, None, "", None):
            print(json.dumps(record, ensure_ascii=False))
        return

    stored_modified, previous = load_state(path)
    baseline = None if (args.full or not previous) else previous
    current: dict[str, tuple[str, str]] = {}
    seen_modified = ""
    for record in fetch_ratings(args.limit, args.timeout, baseline,
                                "" if args.full else stored_modified, current):
        seen_modified = record.get("publisher_updated_at") or seen_modified
        print(json.dumps(record, ensure_ascii=False))
    # Only after every record is written: an aborted run re-fetches instead of skipping.
    # An empty `current` means a 304 (or a capped smoke run): keep the existing baseline.
    if current:
        save_state(path, seen_modified or stored_modified, current)


if __name__ == "__main__":
    main()
