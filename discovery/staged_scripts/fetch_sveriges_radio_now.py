#!/usr/bin/env python3
"""Sveriges Radio — previous/current/next scheduled episodes.

Five-minute snapshots expose programme mix, schedule drift, and interruption signatures.
Verified live 2026-09-04 against the official API using P1 (channel 132). The all-channel
route returns only channel names; a channel ID is required to receive episode details.
Publisher timestamps use legacy ``/Date(milliseconds)/`` strings and are retained
verbatim. Follow Sveriges Radio API terms.

Stdlib only.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "sveriges_radio_schedule_now"
URL = "https://api.sr.se/api/v2/scheduledepisodes/rightnow"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
RELATIONS = (
    "previousscheduledepisode",
    "currentscheduledepisode",
    "nextscheduledepisode",
)


def fetch_schedule(channel_id: int = 132, timeout: int = 30):
    query = urllib.parse.urlencode({"format": "json", "channelid": channel_id})
    request = urllib.request.Request(
        f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    channel = document.get("channel")
    if not isinstance(channel, dict) or channel.get("id") is None:
        raise ValueError("Sveriges Radio response is missing channel")
    for relation in RELATIONS:
        episode = channel.get(relation)
        if not episode:
            continue
        record = dict(episode)
        record.update(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{channel['id']}|{relation}",
                "channel_id": channel["id"],
                "channel_name": channel.get("name"),
                "relation": relation,
            }
        )
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--channel-id", type=int, default=132)
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.limit > 0:
        for index, record in enumerate(fetch_schedule(args.channel_id, args.timeout)):
            if index >= args.limit:
                break
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
