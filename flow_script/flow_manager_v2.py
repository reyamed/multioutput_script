"""
Flow distribution planner for a multi-cluster Elasticsearch topology.

Reads _cat/indices from the SOURCE cluster, rolls indices up into per-flow
future footprints, keeps major/big flows on the source, and distributes the
medium/small flows across two empty target clusters (filled by redirection,
not data movement).

Model summary
-------------
- A flow = the <code> in "log-<code>-<retention>_<date>". A flow can own
  several retention streams; ALL of its indices travel together to one cluster.
- Cadence (bucket period) is a deterministic function of retention, taken from
  the ILM schema: long retention -> monthly buckets, then weekly, then daily.
- Hot = the current (newest) bucket of each retention stream -> always one
  bucket per stream. Cold = every older living bucket. Retention length only
  grows cold, never hot.
- Shards are NOT split by tier: the 100k cap is a master/cluster-state limit,
  so hot+cold shards count against one shard budget per cluster.
- Clusters start empty and fill from redirection. State at T days after the
  one-shot redirect: each stream holds min(T, retention)/period buckets.
    * PLACEMENT is decided at T=1096 (full saturation / endgame) so balance and
      the <50% / <100k guarantees hold once the clusters are full.
    * A T=92 ramp snapshot is reported alongside (3 months after redirection).
- Placement: greedy, biggest-demand-first, each flow to the cluster that keeps
  its most-stressed dimension (hot / cold / shards) lowest.
"""

from elasticsearch import Elasticsearch
import pandas as pd, math
from collections import defaultdict

PLACE_T = 1096   # saturation horizon for PLACEMENT (endgame balance)
RAMP_T  = 92     # snapshot horizon reported alongside (state 3 months after redirection)

# disk budgets = 50% of each pool; shards = one cluster-wide budget
CLUSTERS = {
    "cluster-1": {"hot_gb": 270*1024*0.5,  "cold_gb": 1024*1024*0.5, "sh_budget": 90000, "sh_cap": 100000},
    "cluster-2": {"hot_gb": 400*1024*0.5,  "cold_gb": 1500*1024*0.5, "sh_budget": 90000, "sh_cap": 100000},  # <-- set C2 cold
}

# exact retention (days) -> bucket period (days), straight from the ILM schema
PERIOD = {
    1096: 30, 730: 30, 365: 30, 184: 30, 92: 30,   # monthly
    31: 7,                                          # weekly
    14: 1, 7: 1, 3: 1, 1: 1,                        # daily
}

def classify(gb):
    if gb >= 1024: return "major"
    if gb >= 400:  return "big"
    if gb >= 10:   return "medium"
    return "small"

def parse(name):
    # log-<code>-<ret>_<date>   e.g. log-abc-0092_2026.09   ADAPT if code has dashes
    p = name.split("-")
    ret, date = p[2].split("_", 1)
    return p[1], int(ret), date

def footprint(full_gb, bucket_sh, R, period, T):
    """One retention-stream's state at T days after redirection (fills from empty)."""
    live = math.ceil(min(T, R) / period)      # buckets present at time T
    hot_gb  = full_gb                         # current bucket = one, per stream
    cold_gb = (live - 1) * full_gb
    return hot_gb, cold_gb, live * bucket_sh, live

def collect(es):
    idx = es.cat.indices(format="json", bytes="b", h="index,pri,rep,store.size")
    streams = defaultdict(list)                # (code, R) -> [(date, shards, gb)]
    for i in idx:
        name = i.get("index")
        if not name: continue
        try: code, R, date = parse(name)
        except: continue
        if R not in PERIOD:                    # retention not in the schema -> flag, don't guess
            print(f"WARNING: unknown retention {R} in {name}"); continue
        sh = int(i.get("pri") or 0) * (1 + int(i.get("rep") or 0))
        gb = int(i.get("store.size") or 0) / (1024**3)
        streams[(code, R)].append((date, sh, gb))

    # accumulate every retention-stream up to its flow, so a flow moves as one unit
    flows = defaultdict(lambda: {"R": set(),
        "hot_gb":0.0,"cold_gb":0.0,"sh":0,"live":0,
        "r_hot":0.0,"r_cold":0.0,"r_sh":0,"r_live":0})

    for (code, R), items in streams.items():
        period = PERIOD[R]
        items.sort()                                           # by date, newest last
        completed = items[:-1] if len(items) > 1 else items    # drop half-written current bucket
        full_gb   = sum(g for _,_,g in completed) / len(completed)
        bucket_sh = round(sum(s for _,s,_ in completed) / len(completed))

        p_hot, p_cold, p_sh, p_live = footprint(full_gb, bucket_sh, R, period, PLACE_T)  # endgame
        r_hot, r_cold, r_sh, r_live = footprint(full_gb, bucket_sh, R, period, RAMP_T)   # 3 months

        f = flows[code]; f["R"].add(R)
        f["hot_gb"]+=p_hot; f["cold_gb"]+=p_cold; f["sh"]+=p_sh; f["live"]+=p_live
        f["r_hot"]+=r_hot;  f["r_cold"]+=r_cold;  f["r_sh"]+=r_sh; f["r_live"]+=r_live

    rows = []
    for code, f in flows.items():
        gb = f["hot_gb"] + f["cold_gb"]
        rows.append({
            "flow": code,
            "retentions": ",".join(str(r) for r in sorted(f["R"])),
            "class": classify(gb),                              # on the WHOLE flow
            "hot_gb": round(f["hot_gb"],1), "cold_gb": round(f["cold_gb"],1),
            "gb": round(gb,1), "sh": f["sh"], "live": f["live"],
            "ramp_hot_gb": round(f["r_hot"],1), "ramp_cold_gb": round(f["r_cold"],1),
            "ramp_gb": round(f["r_hot"]+f["r_cold"],1), "ramp_sh": f["r_sh"], "ramp_live": f["r_live"],
        })
    return pd.DataFrame(rows).sort_values("gb", ascending=False)

def distribute(df):
    move = df[df["class"].isin(["medium","small"])].copy()     # one row = one whole flow
    thot = sum(c["hot_gb"]    for c in CLUSTERS.values())
    tcol = sum(c["cold_gb"]   for c in CLUSTERS.values())
    tsh  = sum(c["sh_budget"] for c in CLUSTERS.values())
    # How heavy is the flow in term of hot, cold and shard capacity
    move["demand"] = move.apply(lambda r: max(r["hot_gb"]/thot, r["cold_gb"]/tcol, r["sh"]/tsh), axis=1)
    move = move.sort_values("demand", ascending=False)

    used = {n: {"hot":0.0,"cold":0.0,"sh":0} for n in CLUSTERS}
    out = []
    for _, r in move.iterrows():
        best, bs = None, None
        for n, c in CLUSTERS.items():
            score = max((used[n]["hot"] +r["hot_gb"]) /c["hot_gb"],     # placement uses saturated sizes
                        (used[n]["cold"]+r["cold_gb"])/c["cold_gb"],
                        (used[n]["sh"]  +r["sh"])     /c["sh_budget"])
            if bs is None or score < bs: best, bs = n, score
        used[best]["hot"]+=r["hot_gb"]; used[best]["cold"]+=r["cold_gb"]; used[best]["sh"]+=r["sh"]
        out.append({**r, "cluster": best, "over_budget": bs > 1})
    return pd.DataFrame(out), used

def summary(plan, hot_key, cold_key, sh_key):
    """Per-cluster totals for a given horizon's columns."""
    rows = []
    for n, c in CLUSTERS.items():
        sub = plan[plan["cluster"] == n]
        hot, cold, sh = sub[hot_key].sum(), sub[cold_key].sum(), int(sub[sh_key].sum())
        rows.append({"cluster": n, "flows": len(sub),
            "hot_tb": round(hot/1024,2),   "hot_pct_of_budget":  round(100*hot /c["hot_gb"],1),
            "cold_tb": round(cold/1024,2), "cold_pct_of_budget": round(100*cold/c["cold_gb"],1),
            "shards": sh, "shard_pct_of_cap": round(100*sh/c["sh_cap"],1)})
    return pd.DataFrame(rows)

def export(plan, df, path="distribution_plan.xlsx"):
    cols = ["flow","class","retentions","cluster","over_budget",
            "hot_gb","cold_gb","gb","sh","live",
            "ramp_hot_gb","ramp_cold_gb","ramp_gb","ramp_sh","ramp_live"]
    assignments = plan[cols].sort_values(["cluster","gb"], ascending=[True,False])
    kept = df[df["class"].isin(["major","big"])].assign(cluster="source")
    with pd.ExcelWriter(path) as w:
        assignments.to_excel(w, sheet_name="flow_assignments", index=False)
        summary(plan, "hot_gb", "cold_gb", "sh").to_excel(w, sheet_name="summary_endgame", index=False)
        summary(plan, "ramp_hot_gb", "ramp_cold_gb", "ramp_sh").to_excel(w, sheet_name="summary_3months", index=False)
        kept.to_excel(w, sheet_name="kept_on_source", index=False)
    return path

if __name__ == "__main__":
    es = Elasticsearch("http://localhost:9200")
    df = collect(es)
    plan, used = distribute(df)
    print(export(plan, df))