import csv
from elasticsearch import Elasticsearch, helpers

ES = "http://localhost:9200"
INDEX = "tickets"
OUT = "export.csv"
PAGE = 1000
FIELDS = ["id", "title", "status", "priority", "assignee", "created_at"]

QUERY = {
    "bool": {
        "filter": [
            {"terms": {"status": ["open", "in_progress"]}},
            {"terms": {"priority": ["high", "critical"]}},
            {"range": {"created_at": {"gte": "now-90d"}}},
        ]
    }
}

es = Elasticsearch(ES, request_timeout=60, retry_on_timeout=True, max_retries=3)
# es = Elasticsearch(ES, basic_auth=("elastic", "password"))
# es = Elasticsearch("https://my-deployment.es.io:9243", api_key="xxxxx")

hits = helpers.scan(
    es,
    index=INDEX,
    query={"query": QUERY},
    source=FIELDS,          # only fetch the columns we write
    size=PAGE,              # docs per shard per batch
    preserve_order=False,   # no global sort = much faster
    scroll="5m",
    request_timeout=60,
)

f = open(OUT, "w", newline="", encoding="utf-8")
w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
w.writeheader()

total = 0
for h in hits:
    w.writerow(h["_source"])
    total += 1
    if total % 10000 == 0:
        print(total, "rows")

f.close()
es.close()
print("done ->", OUT, total, "rows")