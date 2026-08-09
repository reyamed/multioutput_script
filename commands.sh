# Segments per topic, ranked
cd /path/to/broker   # your log.dirs entry

find . -name '*.index' \
  | sed 's|^\./||; s|-[0-9]\+/.*||' \
  | sort | uniq -c | sort -rn | head -20

# Worst individual partitions
for d in */; do
  echo "$(ls "$d" | grep -c '\.log$') ${d%/}"
done | sort -rn | head -20


# Total, to compare against the limit
echo "segments: $(find . -name '*.log' | wc -l)"
echo "mappings needed: $(( $(find . -name '*.index' -o -name '*.timeindex' | wc -l) ))"
sysctl -n vm.max_map_count