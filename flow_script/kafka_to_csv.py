#!/usr/bin/env python3
"""
Export Kafka ingestion-over-a-window per (consumer group, topic) from an
Elasticsearch/Metricbeat monitoring cluster to CSV.

Ingestion = delta of the committed offset over the window:
  for each partition, (max offset - min offset) in the window,
  summed across the topic's partitions.

Uses a COMPOSITE aggregation so it paginates through thousands of
(group, topic, partition) combos with no terms `size` cap.

Config via environment variables:
  ES_URL        e.g. https://es-monitoring.mydomain:9200   (required)
  ES_API_KEY    base64 API key            -> sent as "Authorization: ApiKey ..."
    -- or --
  ES_USER / ES_PASS   basic auth
  ES_INDEX      index pattern   (default: metricbeat-*)
  ES_WINDOW     lookback gte    (default: now-1h)
  ES_VERIFY     "false" to skip TLS verification (self-signed)  (default: true)
  OUT           output csv path (default: kafka_ingestion.csv)

Field names (edit here if your mapping differs, e.g. kafka_exporter/Prometheus):
"""

import csv
import os
import sys

import requests
from requests.auth import HTTPBasicAuth

# ---- field mapping (Metricbeat kafka.consumergroup metricset) --------------
F_GROUP     = "kafka.consumergroup.group"
F_TOPIC     = "kafka.consumergroup.topic"
F_PARTITION = "kafka.consumergroup.partition"
F_OFFSET    = "kafka.consumergroup.offset"
F_TIME      = "@timestamp"
# ---------------------------------------------------------------------------

PER_PARTITION = "--per-partition" in sys.argv  # add --per-partition for finer output
PAGE = 1000                                    # composite buckets per page

ES_URL   = os.environ.get("ES_URL", "").rstrip("/")
ES_INDEX = os.environ.get("ES_INDEX", "metricbeat-*")
ES_WINDOW = os.environ.get("ES_WINDOW", "now-1h")
OUT      = os.environ.get("OUT", "kafka_ingestion.csv")
VERIFY   = os.environ.get("ES_VERIFY", "true").lower() != "false"

if not ES_URL:
    sys.exit("ES_URL is not set. export ES_URL=https://your-cluster:9200")

session = requests.Session()
session.verify = VERIFY
if os.environ.get("ES_API_KEY"):
    session.headers["Authorization"] = "ApiKey " + os.environ["ES_API_KEY"]
elif os.environ.get("ES_USER"):
    session.auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ.get("ES_PASS", ""))

if not VERIFY:
    requests.packages.urllib3.disable_warnings()  # silence self-signed noise


def build_body(after=None):
    composite = {
        "size": PAGE,
        "sources": [
            {"group":     {"terms": {"field": F_GROUP}}},
            {"topic":     {"terms": {"field": F_TOPIC}}},
            {"partition": {"terms": {"field": F_PARTITION}}},
        ],
    }
    if after:
        composite["after"] = after
    return {
        "size": 0,
        "query": {"bool": {"filter": [
            {"range": {F_TIME: {"gte": ES_WINDOW, "lte": "now"}}}
        ]}},
        "aggs": {"flows": {
            "composite": composite,
            "aggs": {
                "start": {"min": {"field": F_OFFSET}},
                "end":   {"max": {"field": F_OFFSET}},
            },
        }},
    }


def run():
    url = f"{ES_URL}/{ES_INDEX}/_search"
    # topic-level aggregate: (group, topic) -> [ingested, partition_count]
    per_topic = {}
    # partition-level rows, only kept when --per-partition
    per_part_rows = []
    resets = 0
    after = None
    pages = 0

    while True:
        r = session.post(url, json=build_body(after), timeout=120)
        r.raise_for_status()
        agg = r.json()["aggregations"]["flows"]
        buckets = agg.get("buckets", [])
        if not buckets:
            break
        pages += 1

        for b in buckets:
            k = b["key"]
            group, topic, partition = k["group"], k["topic"], k["partition"]
            start = b["start"]["value"]
            end = b["end"]["value"]
            if start is None or end is None:
                continue
            delta = end - start
            if delta < 0:               # offset reset inside the window
                resets += 1
                delta = 0

            slot = per_topic.setdefault((group, topic), [0, 0])
            slot[0] += delta
            slot[1] += 1

            if PER_PARTITION:
                per_part_rows.append([group, topic, partition, int(delta)])

        sys.stderr.write(f"\rpages={pages}  flows={len(per_topic)}  ")
        sys.stderr.flush()
        after = agg.get("after_key")
        if not after:
            break

    sys.stderr.write("\n")
    if resets:
        sys.stderr.write(
            f"warning: {resets} partition(s) showed a negative delta "
            "(offset reset in window) and were counted as 0.\n"
        )

    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        if PER_PARTITION:
            w.writerow(["consumer_group", "topic", "partition", "ingested_messages"])
            per_part_rows.sort(key=lambda x: (x[0], x[1], x[2]))
            w.writerows(per_part_rows)
        else:
            w.writerow(["consumer_group", "topic", "partitions", "ingested_messages"])
            rows = [[g, t, cnt, int(ing)] for (g, t), (ing, cnt) in per_topic.items()]
            rows.sort(key=lambda x: x[3], reverse=True)  # busiest first
            w.writerows(rows)

    print(f"wrote {OUT}  ({len(per_topic)} group/topic flows)")


if __name__ == "__main__":
    run()