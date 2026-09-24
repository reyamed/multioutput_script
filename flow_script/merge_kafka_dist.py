#!/usr/bin/env python3
"""
Merge Kafka ingestion-per-flow (from the MONITO cluster, via
kafka_ingestion_to_csv.py) with the flow distribution plan (computed against
PROD) to show how much ingestion each target cluster inherits.

Join key = flow code.
  ingestion side : topic = <prefix>-<flow>-<version>, prefix in PREFIXES.
                   one consumer group, so a flow's ingestion is summed over
                   ALL its topics (both prefixes, every version).
  plan side      : 'flow' column, taken from
                     flow_assignments  (medium/small -> cluster-1 / cluster-2)
                   + kept_on_source    (major/big     -> source)

Usage:
  python3 merge_ingestion_distribution.py [ingestion.csv] [plan.json|plan.xlsx]
  defaults: kafka_ingestion.csv  distribution_plan.json

Outputs:
  flow_ingestion.csv     per flow: cluster, class, ingested_messages, #topics, match flags
  cluster_ingestion.csv  per cluster: flows, ingested_messages, pct_of_total
  + a reconciliation summary on stderr (unmatched on both sides)
"""

import csv
import json
import sys
from collections import defaultdict

# topic prefixes that carry a flow. edit if you have others.
PREFIXES = ("direct", "final")


def flow_from_topic(topic):
    """<prefix>-<flow>-<version> -> <flow>.
    Strips a known leading prefix, then strips the trailing -<version> segment.
    Keeps dashes inside the flow code (rsplit only removes the last segment)."""
    head, _, rest = topic.partition("-")
    if _ and head in PREFIXES:
        body = rest                 # drop the prefix
    else:
        body = topic                # unknown prefix -> use the whole name, flagged later
    flow, sep, _version = body.rpartition("-")
    return flow if sep else body    # no version segment -> body is the flow


def load_ingestion(path):
    per_flow = defaultdict(int)
    topics = defaultdict(set)
    unknown_prefix = set()
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            topic = row["topic"].strip()
            if not topic:
                continue
            ing = int(float(row["ingested_messages"]))
            if not topic.partition("-")[0] in PREFIXES:
                unknown_prefix.add(topic)
            flow = flow_from_topic(topic)
            per_flow[flow] += ing
            topics[flow].add(topic)
    return per_flow, topics, unknown_prefix


def load_plan(path):
    """flow -> (cluster, class) from the distribution output (json or xlsx)."""
    mapping = {}
    if path.endswith(".json"):
        doc = json.load(open(path))
        for rec in doc.get("flow_assignments", []):
            mapping[rec["flow"]] = (rec["cluster"], rec.get("class", ""))
        for rec in doc.get("kept_on_source", []):
            mapping[rec["flow"]] = (rec.get("cluster", "source"), rec.get("class", ""))
    elif path.endswith((".xlsx", ".xls")):
        import pandas as pd
        xl = pd.ExcelFile(path)
        for _, r in xl.parse("flow_assignments").iterrows():
            mapping[r["flow"]] = (r["cluster"], r.get("class", ""))
        if "kept_on_source" in xl.sheet_names:
            for _, r in xl.parse("kept_on_source").iterrows():
                mapping[r["flow"]] = (r.get("cluster", "source"), r.get("class", ""))
    else:
        sys.exit("plan file must be .json or .xlsx")
    return mapping


def main():
    ing_path = sys.argv[1] if len(sys.argv) > 1 else "kafka_ingestion.csv"
    plan_path = sys.argv[2] if len(sys.argv) > 2 else "distribution_plan.json"

    per_flow, topics, unknown_prefix = load_ingestion(ing_path)
    plan = load_plan(plan_path)

    rows = []
    for flow in set(per_flow) | set(plan):
        cluster, cls = plan.get(flow, ("UNMATCHED", ""))
        rows.append({
            "flow": flow,
            "cluster": cluster,
            "class": cls,
            "ingested_messages": per_flow.get(flow, 0),
            "topics": len(topics.get(flow, ())),
            "in_plan": flow in plan,
            "in_ingestion": flow in per_flow,
        })
    rows.sort(key=lambda r: r["ingested_messages"], reverse=True)

    with open("flow_ingestion.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "flow", "cluster", "class", "ingested_messages",
            "topics", "in_plan", "in_ingestion"])
        w.writeheader()
        w.writerows(rows)

    per_cluster = defaultdict(lambda: [0, 0])       # cluster -> [ingested, flows]
    for r in rows:
        per_cluster[r["cluster"]][0] += r["ingested_messages"]
        per_cluster[r["cluster"]][1] += 1
    total = sum(v[0] for v in per_cluster.values()) or 1

    with open("cluster_ingestion.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cluster", "flows", "ingested_messages", "pct_of_total"])
        for cl, (ing, n) in sorted(per_cluster.items(), key=lambda x: -x[1][0]):
            w.writerow([cl, n, ing, round(100 * ing / total, 1)])

    # ---- reconciliation -----------------------------------------------------
    no_ing = sorted(f for f in plan if per_flow.get(f, 0) == 0)
    no_plan = sorted(f for f in per_flow if f not in plan)

    e = sys.stderr
    e.write("\n=== ingestion per cluster (last window) ===\n")
    for cl, (ing, n) in sorted(per_cluster.items(), key=lambda x: -x[1][0]):
        e.write(f"  {cl:<12} {n:>5} flows   {ing:>16,} msgs   {100*ing/total:>5.1f}%\n")
    e.write(f"  {'TOTAL':<12} {len(rows):>5} flows   {int(total):>16,} msgs\n")

    if no_plan:
        e.write(f"\n! {len(no_plan)} flow(s) have ingestion but are NOT in the plan "
                "(counted under cluster=UNMATCHED). Likely a code mismatch or a new flow:\n")
        e.write("    " + ", ".join(no_plan[:20]) + (" ..." if len(no_plan) > 20 else "") + "\n")
    if no_ing:
        e.write(f"\n! {len(no_ing)} planned flow(s) had ZERO ingestion in this window "
                "(idle, or code mismatch):\n")
        e.write("    " + ", ".join(no_ing[:20]) + (" ..." if len(no_ing) > 20 else "") + "\n")
    if unknown_prefix:
        e.write(f"\n! {len(unknown_prefix)} topic(s) had no known prefix {PREFIXES} "
                "— check PREFIXES at the top of the script:\n")
        e.write("    " + ", ".join(sorted(unknown_prefix)[:10]) + "\n")

    e.write("\nwrote flow_ingestion.csv and cluster_ingestion.csv\n")


if __name__ == "__main__":
    main()