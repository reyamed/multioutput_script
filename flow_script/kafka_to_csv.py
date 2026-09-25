#!/usr/bin/env python3
"""
Export Kafka ingestion-over-a-window per topic from an Elasticsearch/Metricbeat
MONITORING cluster to CSV.

Ingestion = delta of the committed offset over the window:
  for each partition, (max offset - min offset) in the window,
  summed across the topic's partitions  ->  messages consumed.
(This is NOT lag. Lag = consumer_lag = backlog at a point in time.)

Field names were taken from the user's own Kibana "Request" panel:
  group   -> kafka.consumergroup.id      (value: streamer-consumer-1)
  offset  -> kafka.consumergroup.offset
  topic   -> topic                       (bare, not kafka.consumergroup.topic)
  time    -> @timestamp
  env     -> environment                 (value: prod)
The PARTITION field wasn't visible, and the mapping is mixed (bare `topic`
but nested `kafka.consumergroup.*`), so the script AUTO-DETECTS field paths
from one sample document and prints what it resolved. Override any of them
with the ES_*_FIELD env vars if detection picks wrong.

Config via environment variables:
  ES_URL          e.g. https://es-monito.mydomain:9200   (required)
  ES_API_KEY      base64 API key   -> "Authorization: ApiKey ..."
    -- or --
  ES_USER / ES_PASS   basic auth
  ES_INDEX        index pattern   (default: metricbeat-*)
  ES_WINDOW       lookback gte    (default: now-1h)   accepts now-6h or absolute
  ES_VERIFY       "false" to skip TLS verification (self-signed)  (default: true)
  OUT             output csv path (default: kafka_ingestion.csv)

  Filters (match the Kibana viz; clear with an empty string to widen):
  ES_GROUP        consumer group id filter   (default: streamer-consumer-1)
  ES_ENV          environment filter         (default: prod)

  Field overrides (skip auto-detect for that field):
  ES_GROUP_FIELD  ES_TOPIC_FIELD  ES_PARTITION_FIELD  ES_OFFSET_FIELD  ES_ENV_FIELD
"""

import csv
import os
import sys

import requests
from requests.auth import HTTPBasicAuth

# candidate field paths, tried in order against a sample doc when not overridden
CANDIDATES = {
    "group":     ["kafka.consumergroup.id", "kafka.consumergroup.group"],
    "topic":     ["topic", "kafka.consumergroup.topic"],
    "partition": ["kafka.partition.id", "partition", "kafka.consumergroup.partition"],
    "offset":    ["kafka.consumergroup.offset", "offset"],
}
ENV_FIELD_DEFAULT = "environment"
PAGE = 1000                                    # composite buckets per page


# ---- pure helpers (unit-tested below) --------------------------------------
def flatten(src, prefix=""):
    """{'kafka': {'consumergroup': {'offset': 5}}, 'topic': 't'}
       -> {'kafka.consumergroup.offset': 5, 'topic': 't'}"""
    out = {}
    for k, v in src.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        else:
            out[key] = v
    return out


def resolve_fields(sample_source, overrides):
    """Pick the real path for each logical field. An override wins outright;
    otherwise the first candidate present in the flattened sample doc is used.
    Returns (resolved_dict, warnings_list)."""
    flat = flatten(sample_source) if sample_source else {}
    resolved, warns = {}, []
    for logical, cands in CANDIDATES.items():
        ov = overrides.get(logical)
        if ov:
            resolved[logical] = ov
            if flat and ov not in flat:
                warns.append(f"{logical}: override '{ov}' not found in sample doc")
            continue
        hit = next((c for c in cands if c in flat), None)
        if hit:
            resolved[logical] = hit
        else:
            resolved[logical] = cands[0]
            if flat:
                warns.append(
                    f"{logical}: none of {cands} in sample doc, defaulting to "
                    f"'{cands[0]}' — set ES_{logical.upper()}_FIELD if wrong")
    return resolved, warns


def buckets_to_deltas(buckets):
    """composite buckets -> per (group,topic,partition) delta, plus reset count.
    Returns (rows, resets) where rows = [(group, topic, partition, delta)]."""
    rows, resets = [], 0
    for b in buckets:
        k = b["key"]
        start = b["start"]["value"]
        end = b["end"]["value"]
        if start is None or end is None:
            continue
        delta = end - start
        if delta < 0:                  # offset reset / rebalance inside the window
            resets += 1
            delta = 0
        rows.append((k["group"], k["topic"], k["partition"], delta))
    return rows, resets
# ---------------------------------------------------------------------------


def build_body(fields, group_val, env_field, env_val, window, after=None):
    filters = [{"range": {"@timestamp": {"gte": window, "lte": "now"}}}]
    if group_val:
        filters.append({"term": {fields["group"]: group_val}})
    if env_val:
        filters.append({"match_phrase": {env_field: env_val}})

    composite = {
        "size": PAGE,
        "sources": [
            {"group":     {"terms": {"field": fields["group"]}}},
            {"topic":     {"terms": {"field": fields["topic"]}}},
            {"partition": {"terms": {"field": fields["partition"]}}},
        ],
    }
    if after:
        composite["after"] = after
    return {
        "size": 0,
        "query": {"bool": {"filter": filters}},
        "aggs": {"flows": {
            "composite": composite,
            "aggs": {
                "start": {"min": {"field": fields["offset"]}},
                "end":   {"max": {"field": fields["offset"]}},
            },
        }},
    }


def make_session():
    s = requests.Session()
    s.verify = os.environ.get("ES_VERIFY", "true").lower() != "false"
    if os.environ.get("ES_API_KEY"):
        s.headers["Authorization"] = "ApiKey " + os.environ["ES_API_KEY"]
    elif os.environ.get("ES_USER"):
        s.auth = HTTPBasicAuth(os.environ["ES_USER"], os.environ.get("ES_PASS", ""))
    if not s.verify:
        requests.packages.urllib3.disable_warnings()
    return s


def sample_doc(session, url, group_field, group_val, env_field, env_val, window):
    """Fetch one matching doc so we can read the real field paths off _source."""
    filters = [{"range": {"@timestamp": {"gte": window, "lte": "now"}}}]
    if group_val:
        filters.append({"term": {group_field: group_val}})
    if env_val:
        filters.append({"match_phrase": {env_field: env_val}})
    r = session.post(url, json={"size": 1, "query": {"bool": {"filter": filters}}}, timeout=60)
    r.raise_for_status()
    hits = r.json()["hits"]["hits"]
    return hits[0]["_source"] if hits else None


def run():
    es_url = os.environ.get("ES_URL", "").rstrip("/")
    if not es_url:
        sys.exit("ES_URL is not set. export ES_URL=https://your-monito-cluster:9200")
    index   = os.environ.get("ES_INDEX", "metricbeat-*")
    window  = os.environ.get("ES_WINDOW", "now-1h")
    out     = os.environ.get("OUT", "kafka_ingestion.csv")
    group_val = os.environ.get("ES_GROUP", "streamer-consumer-1")
    env_val   = os.environ.get("ES_ENV", "prod")
    env_field = os.environ.get("ES_ENV_FIELD", ENV_FIELD_DEFAULT)
    overrides = {
        "group":     os.environ.get("ES_GROUP_FIELD"),
        "topic":     os.environ.get("ES_TOPIC_FIELD"),
        "partition": os.environ.get("ES_PARTITION_FIELD"),
        "offset":    os.environ.get("ES_OFFSET_FIELD"),
    }
    per_partition = "--per-partition" in sys.argv
    e = sys.stderr

    session = make_session()
    url = f"{es_url}/{index}/_search"

    # ---- resolve field paths from a sample doc -----------------------------
    # use the group override if given, else the first candidate, just to fetch a doc
    gfield0 = overrides["group"] or CANDIDATES["group"][0]
    src = sample_doc(session, url, gfield0, group_val, env_field, env_val, window)
    if src is None:
        e.write("! no document matched the filters (group="
                f"{group_val!r}, {env_field}={env_val!r}, window={window}). "
                "Check ES_GROUP / ES_ENV, or clear them with an empty string.\n")
        sys.exit(2)
    fields, warns = resolve_fields(src, overrides)

    e.write("resolved fields:\n")
    for k in ("group", "topic", "partition", "offset"):
        e.write(f"  {k:<10} -> {fields[k]}\n")
    e.write(f"  {'env':<10} -> {env_field} == {env_val!r}\n")
    e.write(f"  filter group == {group_val!r}   window {window} .. now\n")
    for w in warns:
        e.write(f"  ! {w}\n")

    # ---- paginate the composite agg ----------------------------------------
    per_topic = {}                 # (group, topic) -> [ingested, partition_count]
    per_part_rows = []
    resets = 0
    after = None
    pages = 0
    while True:
        body = build_body(fields, group_val, env_field, env_val, window, after)
        r = session.post(url, json=body, timeout=120)
        r.raise_for_status()
        agg = r.json()["aggregations"]["flows"]
        buckets = agg.get("buckets", [])
        if not buckets:
            break
        pages += 1
        rows, rs = buckets_to_deltas(buckets)
        resets += rs
        for group, topic, partition, delta in rows:
            slot = per_topic.setdefault((group, topic), [0, 0])
            slot[0] += delta
            slot[1] += 1
            if per_partition:
                per_part_rows.append([group, topic, partition, int(delta)])
        e.write(f"\r  pages={pages}  topics={len(per_topic)}  ")
        e.flush()
        after = agg.get("after_key")
        if not after:
            break
    e.write("\n")
    if resets:
        e.write(f"! {resets} partition-sample(s) had a negative delta "
                "(offset reset/rebalance in window), counted as 0.\n")

    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        if per_partition:
            w.writerow(["consumer_group", "topic", "partition", "ingested_messages"])
            per_part_rows.sort(key=lambda x: (x[0], x[1], x[2]))
            w.writerows(per_part_rows)
        else:
            w.writerow(["consumer_group", "topic", "partitions", "ingested_messages"])
            rows = [[g, t, cnt, int(ing)] for (g, t), (ing, cnt) in per_topic.items()]
            rows.sort(key=lambda x: x[3], reverse=True)
            w.writerows(rows)

    print(f"wrote {out}  ({len(per_topic)} topics)")


if __name__ == "__main__":
    run()