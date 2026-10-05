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