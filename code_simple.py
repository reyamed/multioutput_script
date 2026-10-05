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