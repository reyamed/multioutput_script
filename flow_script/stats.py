def get_flow_stats(es, prefix, cluster_prefix, index_pattern, portfolio, page_size=1000):
    index_pattern_cc = f"{cluster_prefix}:{index_pattern}"
    logging.debug('Getting stats for index pattern "%s".', index_pattern_cc)

    query_body = globals()[f"STATS_QUERY_{prefix.upper()}"]
    aggs = query_body["aggs"]
    composite = aggs["multiagg"]["composite"]
    composite["size"] = page_size
    composite.pop("after", None)

    while True:
        try:
            result = es.search(
                index=index_pattern_cc,
                aggs=aggs,
                size=0,
                track_total_hits=False,
                request_cache=False,
                filter_path=[
                    "aggregations.multiagg.after_key",
                    "aggregations.multiagg.buckets.key",
                    "aggregations.multiagg.buckets.doc_count",
                    # add any sub-agg fields here if you introduce them
                ],
            )
        except ApiError:
            logging.exception("Stats query failed for %s", index_pattern_cc)
            raise

        agg = result.get("aggregations", {}).get("multiagg", {})
        buckets = agg.get("buckets", [])
        if not buckets:
            break

        for bucket in buckets:
            doc = bucket.pop("key")
            doc.update(bucket)                 # keeps doc_count etc.
            doc.setdefault("portfolio", portfolio)
            yield doc

        after = agg.get("after_key")
        if after is None or len(buckets) < page_size:
            break
        composite["after"] = after