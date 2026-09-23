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
    * PLACEMENT is decided on the fully-saturated size (SAT_T, computed but not
      reported) so a long-retention flow can't overflow a cluster months later.
    * Reported horizons are the ramp view: 2 weeks, 1 month, 3 months.
- Placement: greedy, biggest-demand-first, each flow to the cluster that keeps
  its most-stressed dimension (hot / cold / shards) lowest.
"""

from elasticsearch import Elasticsearch
import pandas as pd, math
from collections import defaultdict

# horizons in days, measured from the one-shot redirect. SAT_T is the saturated
# size used only as the PLACEMENT basis and the source-drain baseline (not shown).
# The three reported horizons are the ramp view.
SAT_T   = 1096   # fully saturated (placement basis + source baseline) — not reported
EARLY_T = 14     # 2 weeks
MONTH_T = 30     # 1 month
RAMP_T  = 92     # 3 months

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

def bucket_start_ms(date_str):
    """Epoch ms for the start of the bucket encoded in the index name's date.
    2026.09 -> month start; 2026.09.01 -> that day; 2026.09.w3 -> month start + 2 weeks."""
    import datetime as dt
    parts = date_str.split(".")
    y, mo = int(parts[0]), int(parts[1])
    base = dt.datetime(y, mo, 1, tzinfo=dt.timezone.utc)
    if len(parts) == 3:
        tail = parts[2]
        if tail.lower().startswith("w"):
            base += dt.timedelta(weeks=int(tail[1:]) - 1)     # weekly
        else:
            base = dt.datetime(y, mo, int(tail), tzinfo=dt.timezone.utc)  # daily
    return int(base.timestamp() * 1000)

def read_indices(es):
    """Real per-index records straight from _cat/indices: actual shards, actual
    size, and a deletion date. Deletion = creation + retention (what the ILM
    script keys on); we use the real creation.date when present, else the bucket
    date from the name. This is the ground truth the source drain is measured on."""
    idx = es.cat.indices(format="json", bytes="b", h="index,pri,rep,store.size,creation.date")
    recs = []
    for i in idx:
        name = i.get("index")
        if not name: continue
        try: code, R, date = parse(name)
        except: continue
        if R not in PERIOD:
            print(f"WARNING: unknown retention {R} in {name}"); continue
        creation = int(i.get("creation.date") or 0) or bucket_start_ms(date)
        recs.append({
            "name": name, "code": code, "R": R, "date": date,
            "shards": int(i.get("pri") or 0) * (1 + int(i.get("rep") or 0)),
            "gb": int(i.get("store.size") or 0) / (1024**3),
            "creation_ms": creation,
            "deletion_ms": creation + R * 86400_000,   # aged out R days after creation
        })
    return recs

def footprint(full_gb, bucket_sh, R, period, T):
    """One retention-stream's state at T days after redirection (fills from empty)."""
    live = math.ceil(min(T, R) / period)      # buckets present at time T
    hot_gb  = full_gb                         # current bucket = one, per stream
    cold_gb = (live - 1) * full_gb
    return hot_gb, cold_gb, live * bucket_sh, live

def collect(recs):
    """Per-flow FORWARD projections used for placement and the (empty) new-cluster
    summaries. Takes the real records from read_indices(). These are projections by
    necessity: the new clusters don't hold this data yet."""
    streams = defaultdict(list)                # (code, R) -> [(date, shards, gb)]
    for r in recs:
        streams[(r["code"], r["R"])].append((r["date"], r["shards"], r["gb"]))

    # accumulate every retention-stream up to its flow, so a flow moves as one unit
    flows = defaultdict(lambda: {"R": set(),
        "hot_gb":0.0,"cold_gb":0.0,"sh":0,"live":0,
        "r_hot":0.0,"r_cold":0.0,"r_sh":0,"r_live":0,
        "mo_hot":0.0,"mo_cold":0.0,"mo_sh":0,"mo_live":0,
        "e_hot":0.0,"e_cold":0.0,"e_sh":0,"e_live":0})

    for (code, R), items in streams.items():
        period = PERIOD[R]
        items.sort()                                           # by date, newest last
        completed = items[:-1] if len(items) > 1 else items    # drop half-written current bucket
        full_gb   = sum(g for _,_,g in completed) / len(completed)
        bucket_sh = round(sum(s for _,s,_ in completed) / len(completed))

        p_hot, p_cold, p_sh, p_live = footprint(full_gb, bucket_sh, R, period, SAT_T)    # saturated
        r_hot, r_cold, r_sh, r_live = footprint(full_gb, bucket_sh, R, period, RAMP_T)   # 3 months
        o_hot, o_cold, o_sh, o_live = footprint(full_gb, bucket_sh, R, period, MONTH_T)  # 1 month
        e_hot, e_cold, e_sh, e_live = footprint(full_gb, bucket_sh, R, period, EARLY_T)  # 2 weeks

        f = flows[code]; f["R"].add(R)
        f["hot_gb"]+=p_hot; f["cold_gb"]+=p_cold; f["sh"]+=p_sh; f["live"]+=p_live
        f["r_hot"]+=r_hot;  f["r_cold"]+=r_cold;  f["r_sh"]+=r_sh; f["r_live"]+=r_live
        f["mo_hot"]+=o_hot; f["mo_cold"]+=o_cold; f["mo_sh"]+=o_sh; f["mo_live"]+=o_live
        f["e_hot"]+=e_hot;  f["e_cold"]+=e_cold;  f["e_sh"]+=e_sh; f["e_live"]+=e_live

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
            "month_hot_gb": round(f["mo_hot"],1), "month_cold_gb": round(f["mo_cold"],1),
            "month_gb": round(f["mo_hot"]+f["mo_cold"],1), "month_sh": f["mo_sh"], "month_live": f["mo_live"],
            "early_hot_gb": round(f["e_hot"],1), "early_cold_gb": round(f["e_cold"],1),
            "early_gb": round(f["e_hot"]+f["e_cold"],1), "early_sh": f["e_sh"], "early_live": f["e_live"],
        })
    return pd.DataFrame(rows).sort_values("gb", ascending=False)

def distribute(df):
    move = df[df["class"].isin(["medium","small"])].copy()     # one row = one whole flow
    thot = sum(c["hot_gb"]    for c in CLUSTERS.values())
    tcol = sum(c["cold_gb"]   for c in CLUSTERS.values())
    tsh  = sum(c["sh_budget"] for c in CLUSTERS.values())
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

# internal footprint keys -> clear per-horizon display names for MOVED flows.
# Reported horizons are the ramp view: 2 weeks / 1 month / 3 months. The saturated
# size (placement basis) and hot+cold totals / live counts are intentionally not shown.
DISPLAY = {
    "early_hot_gb": "hot_gb_2wk", "early_cold_gb": "cold_gb_2wk", "early_sh": "sh_2wk",
    "month_hot_gb": "hot_gb_1mo", "month_cold_gb": "cold_gb_1mo", "month_sh": "sh_1mo",
    "ramp_hot_gb":  "hot_gb_3mo", "ramp_cold_gb":  "cold_gb_3mo", "ramp_sh":  "sh_3mo",
}
SHOW_COLS = ["flow","class","retentions","cluster"] + list(DISPLAY)

# kept flows are already saturated on the running source cluster, so they get one
# steady footprint (not a ramp): their saturated hot/cold/shards under plain names.
KEPT_DISPLAY = {"hot_gb": "hot_gb", "cold_gb": "cold_gb", "sh": "shards"}
KEPT_COLS = ["flow","class","retentions","cluster","hot_gb","cold_gb","sh"]

def source_evolution(recs, df, now_ms=None):
    """How the SOURCE cluster evolves after the one-shot redirect — MEASURED from
    the real indices, not modeled.

    Each real index has an actual shard count, size, and deletion date
    (creation + retention). medium/small flows are redirected, so no new indices
    are written for them on source: the indices that exist today are their entire
    remaining life there, and they simply age out -> pure measured drain. major/big
    stay and keep ingesting, so what ages out is replaced; we hold their measured
    current total flat (steady state) rather than pretend they drain. 'kept' =
    major/big floor, 'draining' = medium/small still alive at each horizon."""
    import time
    now_ms = now_ms if now_ms is not None else int(time.time()*1000)
    day = 86400_000
    cls = dict(zip(df["flow"], df["class"]))

    keep = [r for r in recs if cls.get(r["code"]) in ("major","big")]
    move = [r for r in recs if cls.get(r["code"]) in ("medium","small")]
    keep_sh = sum(r["shards"] for r in keep)          # held flat (measured)
    keep_gb = sum(r["gb"]     for r in keep)

    def alive(T):                                     # real indices not yet deleted at now+T
        cut = now_ms + T*day
        surv = move if T == 0 else [r for r in move if r["deletion_ms"] > cut]
        return sum(r["shards"] for r in surv), sum(r["gb"] for r in surv)

    rows = []
    for label, T in [("at_redirect",0),("2weeks",EARLY_T),("1month",MONTH_T),("3months",RAMP_T)]:
        dsh, dgb = alive(T)
        rows.append({"horizon": label, "days": T,
            "kept_tb": round(keep_gb/1024,2), "kept_shards": int(keep_sh),
            "draining_tb": round(dgb/1024,2), "draining_shards": int(dsh),
            "total_tb": round((keep_gb+dgb)/1024,2), "total_shards": int(keep_sh+dsh)})
    return pd.DataFrame(rows)

def build_tables(plan, df, recs):
    """Assemble the same tables both exporters use: assignments, per-horizon
    summaries, kept-on-source, and the MEASURED source drain — as DataFrames."""
    ordered = plan.sort_values(["cluster","gb"], ascending=[True,False])
    assignments = ordered[SHOW_COLS].rename(columns=DISPLAY)
    kept = df[df["class"].isin(["major","big"])].assign(cluster="source")
    kept = kept.sort_values("gb", ascending=False)[KEPT_COLS].rename(columns=KEPT_DISPLAY)
    return {
        "flow_assignments": assignments,
        "summary_2weeks":   summary(plan, "early_hot_gb", "early_cold_gb", "early_sh"),
        "summary_1month":   summary(plan, "month_hot_gb", "month_cold_gb", "month_sh"),
        "summary_3months":  summary(plan, "ramp_hot_gb", "ramp_cold_gb", "ramp_sh"),
        "kept_on_source":   kept,
        "source_evolution": source_evolution(recs, df),
    }

def export_xlsx(plan, df, recs, path="distribution_plan.xlsx"):
    t = build_tables(plan, df, recs)
    with pd.ExcelWriter(path) as w:
        for sheet in ("flow_assignments","summary_2weeks","summary_1month","summary_3months","source_evolution","kept_on_source"):
            t[sheet].to_excel(w, sheet_name=sheet, index=False)
    return path

def export_json(plan, df, recs, path="distribution_plan.json"):
    import json
    t = build_tables(plan, df, recs)
    # cluster config echoed so the frontend has the budgets/caps without a second file
    clusters = {n: {"hot_gb": c["hot_gb"], "cold_gb": c["cold_gb"],
                    "sh_budget": c["sh_budget"], "sh_cap": c["sh_cap"]}
                for n, c in CLUSTERS.items()}
    doc = {
        "clusters": clusters,
        "horizons": {"early_days": EARLY_T, "month_days": MONTH_T, "ramp_days": RAMP_T,
                     "placement_basis_days": SAT_T},
        **{k: v.to_dict(orient="records") for k, v in t.items()},
    }
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)
    return path

def export(plan, df, recs, path=None, fmt="xlsx"):
    """fmt = 'xlsx', 'json', or 'both'."""
    if fmt == "json":
        return export_json(plan, df, recs, path or "distribution_plan.json")
    if fmt == "both":
        return [export_xlsx(plan, df, recs), export_json(plan, df, recs)]
    return export_xlsx(plan, df, recs, path or "distribution_plan.xlsx")

if __name__ == "__main__":
    import sys
    fmt = sys.argv[1] if len(sys.argv) > 1 else "xlsx"   # xlsx | json | both
    es = Elasticsearch("http://localhost:9200")
    recs = read_indices(es)      # real per-index records (ground truth for the drain)
    df = collect(recs)           # per-flow projections (placement + new-cluster fill)
    plan, used = distribute(df)
    print(export(plan, df, recs, fmt=fmt))