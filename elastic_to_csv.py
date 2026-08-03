import csv
import requests

ES = "http://localhost:9200"
INDEX = "tickets"
OUT = "exporimport csv
import requests

ES = "http://localhost:9200"
INDEX = "tickets"
OUT = "export.csv"
PAGE = 1000
FIELDS = ["id", "title", "status", "priority", "assignee", "created_at"]
AUTH = None  # ("elastic", "password")

QUERY = {
    "bool": {
        "filter": [
            {"terms": {"status": ["open", "in_progress"]}},
            {"terms": {"priority": ["high", "critical"]}},
            {"range": {"created_at": {"gte": "now-90d"}}},
        ]
    }
}

s = requests.Session()
s.auth = AUTH
s.headers.update({"Content-Type": "application/json"})

# point in time = stable snapshot, no scroll contexts left behind
pit = s.post(ES + "/" + INDEX + "/_pit?keep_alive=5m").json()["id"]

body = {
    "size": PAGE,
    "query": QUERY,
    "_source": FIELDS,  # only fetch what we write
    "sort": [{"_shard_doc": "asc"}],  # cheapest sort, no scoring
    "track_total_hits": False,  # skip counting everything
    "pit": {"id": pit, "keep_alive": "5m"},
}

f = open(OUT, "w", newline="", encoding="utf-8")
w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
w.writeheader()

total = 0
while True:
    r = s.post(ES + "/_search", json=body)
    r.raise_for_status()
    data = r.json()
    hits = data["hits"]["hits"]
    if not hits:
        break
    for h in hits:
        w.writerow(h["_source"])
    total += len(hits)
    print(total, "rows")
    body["search_after"] = hits[-1]["sort"]
    body["pit"]["id"] = data["pit_id"]

f.close()
s.delete(ES + "/_pit", json={"id": body["pit"]["id"]})
print("done ->", OUT, total, "rows")t.csv"
PAGE = 1000
FIELDS = ["id", "title", "status", "priority", "assignee", "created_at"]
AUTH = None  # ("elastic", "password")

QUERY = {
    "bool": {
        "filter": [
            {"terms": {"status": ["open", "in_progress"]}},
            {"terms": {"priority": ["high", "critical"]}},
            {"range": {"created_at": {"gte": "now-90d"}}},
        ]
    }
}

s = requests.Session()
s.auth = AUTH
s.headers.update({"Content-Type": "application/json"})

# point in time = stable snapshot, no scroll contexts left behind
pit = s.post(ES + "/" + INDEX + "/_pit?keep_alive=5m").json()["id"]

body = {
    "size": PAGE,
    "query": QUERY,
    "_source": FIELDS,              # only fetch what we write
    "sort": [{"_shard_doc": "asc"}],  # cheapest sort, no scoring
    "track_total_hits": False,      # skip counting everything
    "pit": {"id": pit, "keep_alive": "5m"},
}

f = open(OUT, "w", newline="", encoding="utf-8")
w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
w.writeheader()

total = 0
while True:
    r = s.post(ES + "/_search", json=body)
    r.raise_for_status()
    data = r.json()
    hits = data["hits"]["hits"]
    if not hits:
        break
    for h in hits:
        w.writerow(h["_source"])
    total += len(hits)
    print(total, "rows")
    body["search_after"] = hits[-1]["sort"]
    body["pit"]["id"] = data["pit_id"]

f.close()
s.delete(ES + "/_pit", json={"id": body["pit"]["id"]})
print("done ->", OUT, total, "rows")