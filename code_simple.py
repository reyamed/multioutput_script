def create_shards_component(
    self, es, num_shards, shards_component_name, max_shards, execute
):
    num_shards, shards_component_name = self.check_max_shards(
        es, num_shards, shards_component_name, max_shards
    )

    if self._exist_component_template(es, shards_component_name):
        logging.info(f"Component shard {shards_component_name} already exist.")
        return

    try:
        index_settings = {"number_of_shards": str(num_shards)}

        # Max shards per node from the default settings template
        default_tpl = es.cluster.get_component_template(
            name="default_index_settings*"
        )["component_templates"][0]["component_template"]["template"]["settings"]["index"]
        max_per_node = int(
            default_tpl["routing"]["allocation"].get("total_shards_per_node", 1)
        )

        # Count data-hot nodes that store shards
        dh_node_count = sum(
            1
            for node in es.nodes.info()["nodes"].values()
            if "-dh_" in node["name"]
        )

        total_shards = 2 * num_shards  # primaries + replicas
        cluster_capacity = max_per_node * dh_node_count

        # If the shards won't fit, bump total_shards_per_node
        if total_shards >= cluster_capacity:
            new_per_node = total_shards // cluster_capacity + 1
            index_settings["routing"] = {
                "allocation": {"total_shards_per_node": str(new_per_node)}
            }

        body = {"template": {"settings": {"index": index_settings}}}
        logging.info(f"New shard component created: {body}")

        if execute:
            es.cluster.put_component_template(
                name=f"{shards_component_name}", body=body
            )

    except Exception as error:
        raise RuntimeError(
            f"Couldn't create shard component {shards_component_name}, error: {error}"
        ) from error



def _get_period_indices(es, pattern_date, period_matches):
    """Shared body for daily/weekly/monthly index lookups.

    :param pattern_date: regex with one capture group for the date part
    :param period_matches: predicate on the captured group -> bool
    :returns: list of index dicts, largest store_size first
    """
    indices_info = es.cat.indices(
        format="json", h=["index", "store.size", "pri", "rep"]
    )

    indices = []
    for index in indices_info:
        name = index["index"]
        if not (name.startswith("logs") or name.startswith("error")):
            continue
        match = re.match(pattern_date, name)
        if not match or not period_matches(match.group(1)):
            continue
        indices.append({
            "index": name,
            "store_size": size_to_gb(index["store.size"]) if index["store.size"] else 0,
            "pri_shards": int(index["pri"]),
            "rep_shards": int(index["rep"]),
        })

    return sorted(indices, key=lambda x: x["store_size"], reverse=True)

def get_daily_indices(es):
    today = str(date.today())
    pattern = r".*(\d{4}\.\d{2}\.\d{2})$"
    try:
        return _get_period_indices(
            es, pattern, lambda d: d.replace(".", "-") == today
        )
    except Exception as e:
        logging.error(f"Couldn't get daily indices, error: {e}")
        return []


def get_weekly_indices(es):
    week_num = get_week_of_month(datetime.today())
    current_week = date.today().strftime("%Y.%m") + f".w{week_num}"
    pattern = rf".*(\d{{4}}\.\d{{2}}\.w{week_num})$"
    try:
        return _get_period_indices(es, pattern, lambda d: d == current_week)
    except Exception as e:
        logging.error(f"Couldn't get weekly indices, error: {e}")
        return []


def get_monthly_indices(es):
    current_month = date.today().strftime("%Y.%m")
    pattern = r".*(\d{4}\.\d{2})$"
    try:
        return _get_period_indices(es, pattern, lambda d: d == current_month)
    except Exception as e:
        logging.error(f"Couldn't get monthly indices, error: {e}")
        return []


def rollover_index(es, index, env, max_shards, limit_size, token, execute=None):
    latest_index = get_latest_rollover_index(es, env, index, token)

    # Optimal shard count for the index (at least 1)
    raw_optimal = round(latest_index["store_size"] / (limit_size * 2))
    num_optimal_shards = max(raw_optimal, 1)

    current_shards_number = latest_index["pri_shards"]

    # Already optimal (or over) → nothing to do
    if current_shards_number >= num_optimal_shards:
        return

    optimal_shards_name = f"{num_optimal_shards}_shards"

    logging.info(f"******* Base index: {index['index']} ********")
    logging.info(f"====> Latest index: {latest_index['index']} <======")
    logging.info(
        f"Index '{latest_index['index']}' current size: "
        f"{latest_index['store_size']} GB, shards: {latest_index['pri_shards']}"
    )
    logging.info(f"Index to shard: '{latest_index['index']}'")

    # Clamp to the max allowed shard count
    num_optimal_shards, optimal_shards_name = check_max_shards(
        es, num_optimal_shards, optimal_shards_name, max_shards
    )

    try:
        if index["index"].startswith("logs"):
            change_index_template(
                es, latest_index, num_optimal_shards,
                optimal_shards_name, max_shards, execute
            )

        for group_name in ("default", "bigflow"):
            put_index_rollover(env, token, index["index"], group_name, execute)

        logging.info("*" * 60)

    except Exception as e:
        logging.warning(f"index '{index['index']}' failed!, error {e}")
        FAILED_INDICES[f"{index['index']}"] = e



import re
from datetime import date

# matches the YYYY.MM at the start of the date portion in all three formats
INDEX_DATE_RE = re.compile(r"(\d{4})\.(\d{2})")

def previous_month(ref=None):
    ref = ref or date.today()
    year, month = ref.year, ref.month - 1
    if month == 0:          # January -> December of prior year
        month, year = 12, year - 1
    return year, month

def index_year_month(name):
    m = INDEX_DATE_RE.search(name)
    return (int(m.group(1)), int(m.group(2))) if m else None

def delete_previous_month_indices(db, ref=None):
    target = previous_month(ref)
    names = db.get_all_index_names()          # <- adapt to your DB
    to_delete = [n for n in names if index_year_month(n) == target]
    for n in to_delete:
        db.delete_index(n)                    # <- adapt to your DB
    return to_delete


def _total_store_size(es, name):
    """Store size (GB) of an index plus all its rollovers."""
    return sum(
        size_to_gb(i["store.size"])
        for i in es.cat.indices(
            index=f"{name},{name}_*", format="json",
            h=["index", "store.size", "pri", "rep"],
        )
    )

def _collect_sizes(es, names):
    """Sizes of the candidate indices that actually exist."""
    return [_total_store_size(es, n) for n in names if es.indices.exists(index=n)]

# daily — max over last few days
def get_max_size(self, es, index):
    base = re.match(DATE_PATTERN, index["index"]).group(1)
    names = [f"{base}{(date.today() - timedelta(days=i)).strftime('%Y.%m.%d')}"
             for i in (1, 5)]
    return max([index["store_size"], *_collect_sizes(es, names)])

# weekly — max over last 3 weeks
def get_max_size(es, index):
    base = re.match(WEEK_PATTERN, index["index"]).group(1)
    names = []
    for i in range(3):
        d = datetime.today() - timedelta(weeks=i)
        names.append(f"{base}{d.strftime('%Y.%m')}.w{get_week_of_month(d)}")
    return max(_collect_sizes(es, names), default=0)

# monthly — sum over all collect versions
def get_max_size(es, index):
    result = MONTHLY_PATTERN.dissect(index["index"])
    v = int(result["collect_version"])
    names = [index["index"].replace(f"_v{v}.", f"_v{i}.") for i in range(1, v + 1)]
    return sum(_collect_sizes(es, names))