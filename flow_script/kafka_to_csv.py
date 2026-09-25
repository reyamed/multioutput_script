#!/usr/bin/env python3
"""
Kafka ingestion per FLOW over a window, from the MONITO cluster -> Excel.

Ingestion = sum over a flow's partitions of (max offset - min offset) in the window.
A flow spans several topics: direct-<flow>-<version> and final-<flow>-<version>,
every version, all summed into the one <flow>.
"""

import pandas as pd
from collections import defaultdict
from elasticsearch import Elasticsearch

ES_URL   = "https://your-monito-cluster:9200"
API_KEY  = "your_base64_api_key"          # or basic_auth=("user","pass")
INDEX    = "metricbeat-*"
GROUP    = "streamer-consumer-1"
WINDOW   = {"gte": "now-1h", "lte": "now"}
OUT      = "kafka_ingestion_per_flow.xlsx"

PREFIXES = ("direct", "final")


def flow_of(topic):
    """direct-<flow>_<version> / final-<flow>_<version> -> <flow>.
    Drops a known prefix, then the trailing _<version>; keeps dashes inside the flow."""
    head, sep, rest = topic.partition("-")
    body = rest if (sep and head in PREFIXES) else topic
    flow, sep2, _version = body.rpartition("_")
    return flow if sep2 else body


QUERY = {
    "size": 0,
    "query": {"bool": {"filter": [
        {"term":  {"kafka.consumergroup.id": GROUP}},
        {"range": {"@timestamp": WINDOW}},
    ]}},
    "aggs": {"flows": {
        "composite": {
            "size": 1000,
            "sources": [
                {"topic":     {"terms": {"field": "topic"}}},
                {"partition": {"terms": {"field": "kafka.partition.id"}}},
            ],
        },
        "aggs": {
            "start": {"min": {"field": "kafka.consumergroup.offset"}},
            "end":   {"max": {"field": "kafka.consumergroup.offset"}},
        },
    }},
}


def main():
    es = Elasticsearch(ES_URL, api_key=API_KEY, verify_certs=False)  # basic_auth=("u","p") if no api_key

    per_flow = defaultdict(lambda: [0, set(), 0])   # flow -> [ingested, {topics}, partitions]
    after = None
    while True:
        if after:
            QUERY["aggs"]["flows"]["composite"]["after"] = after
        agg = es.search(index=INDEX, body=QUERY)["aggregations"]["flows"]
        buckets = agg.get("buckets", [])
        if not buckets:
            break
        for b in buckets:
            start, end = b["start"]["value"], b["end"]["value"]
            if start is None or end is None:
                continue
            delta = max(0, end - start)             # negative = offset reset -> 0
            topic = b["key"]["topic"]
            f = per_flow[flow_of(topic)]
            f[0] += delta
            f[1].add(topic)
            f[2] += 1
        after = agg.get("after_key")
        if not after:
            break

    rows = [{"flow": flow, "ingested_messages": int(ing),
             "topics": len(topics), "partitions": parts}
            for flow, (ing, topics, parts) in per_flow.items()]
    df = pd.DataFrame(rows).sort_values("ingested_messages", ascending=False)
    df.to_excel(OUT, index=False)
    print(f"wrote {OUT}  ({len(df)} flows, {df['ingested_messages'].sum():,} events total)")


if __name__ == "__main__":
    main()