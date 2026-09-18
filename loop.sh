#!/bin/bash

URL="http://localhost:9200"
USER="elastic"
PASS="changeme"
INDEX="my-index"
i=1

while true; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -u "$USER:$PASS" \
    -X PUT "$URL/$INDEX/_doc/$i?refresh=true" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":$i,\"message\":\"doc $i\"}")

  if [ "$code" = "201" ] || [ "$code" = "200" ]; then
    echo "doc $i created"
  else
    echo "doc $i FAILED (HTTP $code)"
    exit 1
  fi

  i=$((i+1))
  sleep 2
done

#=SUMPRODUCT(ISNUMBER(SEARCH("dc",A2:A110)) * ((RIGHT(D2:D110,2)="tb")*VALUE(LEFT(D2:D110,LEN(D2:D110)-2))*1024 + (RIGHT(D2:D110,2)="gb")*VALUE(LEFT(D2:D110,LEN(D2:D110)-2))))


#!/bin/bash
set -euo pipefail

export ETCDCTL_API=3

BACKUP_DIR="/var/backups/etcd"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/etcd-${TIMESTAMP}.db"

mkdir -p "$BACKUP_DIR"

etcdctl \
  --endpoints="https://127.0.0.1:2379" \
  --cacert="/etc/etcd/ca.crt" \
  --cert="/etc/etcd/server.crt" \
  --key="/etc/etcd/server.key" \
  snapshot save "$BACKUP_FILE"

etcdctl snapshot status "$BACKUP_FILE" --write-out=table

mc cp "$BACKUP_FILE" "minio/etcd-backups/$(date +%Y/%m/%d)/"

# Keep local copies for 7 days
find "$BACKUP_DIR" -type f -name 'etcd-*.db' -mtime +7 -delete