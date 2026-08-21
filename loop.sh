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