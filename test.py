# Drop-in replacements for build_es(), get_stats() and main().
# Everything else in the script (STATS_QUERY, get_prog_args, setup_logging,
# get_base_indices) stays as is. Add these imports at the top:

import copy
import faulthandler

faulthandler.enable()  # on a segfault, dumps the Python stack of every thread to stderr (Rundeck log)

MAX_WORKERS = 5


def build_es(args):
    """Build ONE Elasticsearch client, shared by all worker threads.

    The client and its urllib3 pool are thread-safe. Size the pool to the
    number of workers so threads don't wait on each other for a connection.
    """
    return elasticsearch.Elasticsearch(
        hosts=[args["cluster_url"]],
        verify_certs=True,
        ca_certs=args["ca_certs"],
        client_cert=args["client_cert"],
        client_key=args["client_key"],
        max_retries=5,
        retry_on_timeout=True,
        request_timeout=7200,
        connections_per_node=MAX_WORKERS,
    )


def get_stats(es, cluster_name, base_index):
    """Get statistics for the given base index (paginates the composite agg).

    :es: shared Elasticsearch client
    :returns: list of documents
    """
    documents = []
    index_pattern = f"{cluster_name}-{base_index}-*,-itgsoc-*_topic_unknown*"

    # Private copy per call: never mutate the module-level STATS_QUERY from threads
    aggs = copy.deepcopy(STATS_QUERY["aggs"])
    page_size = aggs["multiagg"]["composite"]["size"]

    logging.debug(f'Getting stats for base index "{base_index}".')

    while True:
        result = es.search(
            index=index_pattern,
            size=0,
            aggs=aggs,
            filter_path="aggregations.multiagg.after_key,aggregations.multiagg.buckets,-**.buckets.*.count",
        )
        multiagg = result.body.get("aggregations", {}).get("multiagg", {})
        buckets = multiagg.get("buckets", [])

        for bucket in buckets:
            # Move the "key" values at the root of the document
            key = bucket.pop("key")
            bucket.update(key)

            # If the flow provider is None, set a default string
            if bucket["flowprovider"] is None:
                bucket["flowprovider"] = "N/A"

            # >>> keep the rest of your existing per-bucket logic here (old lines ~204-211) <<<

            documents.append(bucket)

        # Composite aggs return at most `size` buckets per call: follow after_key
        after_key = multiagg.get("after_key")
        if len(buckets) < page_size or after_key is None:
            break
        aggs["multiagg"]["composite"]["after"] = after_key

    return documents


def main():
    """Launch the script.

    :returns: 1 in case of error, 0 otherwise
    """
    args = get_prog_args()
    setup_logging(args["loglevel"])

    es = build_es(args)
    failed = False
    try:
        base_indices = get_base_indices(es, args["cluster_name"])
        if not base_indices:  # get_base_indices returns None when it hits an error
            logging.error("No base indices found (or failed to list them).")
            return 1

        bulk_payload = []
        cur_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(get_stats, es, args["cluster_name"], base_index): base_index
                for base_index in sorted(base_indices)
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    docs = future.result()
                except Exception:
                    logging.exception(f'Failed to get stats for base index "{futures[future]}".')
                    failed = True
                    continue
                for document in docs:
                    document["@timestamp"] = cur_datetime
                    bulk_payload.append({"_index": "flows-stats", "_source": document})

        logging.info(f"Sending statistics to ElasticSearch: {len(bulk_payload)} documents")
        helpers.bulk(es, bulk_payload)
    finally:
        es.close()

    return 1 if failed else 0

#python3 -c "import sys, ssl; print(sys.version); print(ssl.OPENSSL_VERSION)"
#pip list | grep -Ei 'elastic|urllib3|orjson|certifi'