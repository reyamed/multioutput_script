#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Collect per-flow statistics from an Elasticsearch cluster into the flows-stats index.

Each base index is processed in a separate worker PROCESS (not a thread), so native
code (OpenSSL, glibc malloc) is never shared between concurrent workers. If a worker
crashes, the pool breaks: the base indices that didn't finish are retried in a fresh
pool with fewer workers, and everything that did finish is already sent.
"""

import argparse
import concurrent.futures
import copy
import faulthandler
import logging
import multiprocessing
import os
import re
import sys
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone

import elasticsearch
from elasticsearch import helpers

# On a segfault, dump the Python stack of the crashing process to stderr (Rundeck log).
# Runs in every worker too, since "spawn" re-imports this module.
faulthandler.enable()

STATS_INDEX = "flows-stats"

STATS_QUERY = {
    "aggs": {
        "multiagg": {
            "composite": {
                "size": 2000,  # overridden by --composite-size
                "sources": [
                    {"flowname": {"terms": {"field": "databus.flow.name"}}},
                    {"flowentity": {"terms": {"field": "databus.flow.owner.entity"}}},
                    {"flowprovider": {"terms": {"field": "databus.flow.provider.name", "missing_bucket": True}}},
                    {"dateref": {"date_histogram": {"field": "@timestamp", "fixed_interval": "1d", "format": "yyyy-MM-dd"}}},
                ],
            },
            "aggs": {
                "streamer": {"stats": {"field": "@size"}},
                "stage1": {"stats": {"field": "databus.event.lineage.stage1.event_size", "missing": 0}},
                "stage2": {"stats": {"field": "databus.event.lineage.stage2.event_size", "missing": 0}},
                "stage3": {"stats": {"field": "databus.event.lineage.stage3.event_size", "missing": 0}},
                "stage4": {"stats": {"field": "databus.event.lineage.stage4.event_size", "missing": 0}},
                "stage5": {"stats": {"field": "databus.event.lineage.stage5.event_size", "missing": 0}},
            },
        }
    }
}

# Client of the current worker process, created once by init_worker()
_WORKER_ES = None


def get_prog_args():
    """Parse the command line arguments.

    :returns: Dict with the arguments
    """

    parser = argparse.ArgumentParser(description="Script to get flow statistics from an ElasticSearch cluster.")

    # >>> CHECK: this part wasn't visible in your photos. Keep YOUR original entries
    # >>> (flag names must match your Rundeck jobs) and only add the last three.
    args = [
        ("--cluster-url", {"dest": "cluster_url", "required": True, "help": "ElasticSearch cluster URL"}),
        ("--cluster-name", {"dest": "cluster_name", "required": True, "help": "Cluster name (index prefix)"}),
        ("--ca-certs", {"dest": "ca_certs", "required": True, "help": "CA certificates file"}),
        ("--client-cert", {"dest": "client_cert", "required": True, "help": "Client certificate file"}),
        ("--client-key", {"dest": "client_key", "required": True, "help": "Client key file"}),
        ("--loglevel", {"dest": "loglevel", "default": "INFO", "help": "Log level (default: INFO)"}),
        # New options (all optional, so existing jobs keep working)
        ("--workers", {"dest": "workers", "type": int, "default": 5, "help": "Worker processes (default: 5)"}),
        ("--composite-size", {"dest": "composite_size", "type": int, "default": 2000,
                              "help": "Buckets per composite page (default: 2000)"}),
        ("--max-attempts", {"dest": "max_attempts", "type": int, "default": 3,
                            "help": "Pools to try when a worker crashes (default: 3)"}),
    ]

    for arg, params in args:
        parser.add_argument(arg, **params)

    return vars(parser.parse_args())


def setup_logging(loglevel):
    """Setup logging for the script.

    :loglevel: Log level
    :returns: Nothing
    """

    # Basic logging: output to stdout with the given loglevel, with the PID to tell workers apart
    logging.basicConfig(level=loglevel, format="%(asctime)s - %(process)d - %(levelname)s - %(message)s")

    # Disable ElasticSearch loggers
    for logger_name in ("elasticsearch", "urllib3", "elastic_transport", "elastic_transport.transport",
                        "elastic_transport.node", "elastic_transport.node_pool"):
        logger = logging.getLogger(logger_name)
        logger.addHandler(logging.NullHandler())
        logger.propagate = False


def build_es(args):
    """Create an ElasticSearch client.

    :args: Parsed command line arguments
    :returns: ElasticSearch client
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
    )


def get_base_indices(es, cluster_name):
    """Get the list of base indices (entity to retention) in Elasticsearch.

    :es: ElasticSearch client
    :cluster_name: Cluster name
    :returns: List of base indices, or None on error
    """

    logging.debug("Getting ElasticSearch base indices.")

    # Get all indices
    try:
        indices = es.indices.get(index=f"{cluster_name}-*")

        # >>> CHECK: lines ~151-158 weren't fully visible in your photos.
        # >>> Keep your original cluster_pattern / pattern lines if they differ.
        cluster_pattern = re.escape(cluster_name)
        pattern = rf"^{cluster_pattern}-(.*)-[^-]*(?:\-[0-9]{{2}})?$"

        base_indices = set()
        for index in indices:
            result = re.search(pattern, index)

            if result is not None:
                base_indices.add(result.group(1))
            else:
                logging.warning(f'Failed identify the base index in "{index}".')

        return list(base_indices)
    except elasticsearch.ConnectionTimeout:
        logging.warning("Failed to get the ElasticSearch indices list.")
    except elasticsearch.TransportError as e:
        logging.warning(f"Error in ElasticSearch while getting indices: {e}.")

    return None


def init_worker(args):
    """Runs once in each worker process: own logging, own client."""

    global _WORKER_ES
    setup_logging(args["loglevel"])
    _WORKER_ES = build_es(args)


def get_stats(cluster_name, base_index, composite_size):
    """Get statistics for the given base index. Runs in a worker process.

    :cluster_name: Cluster name
    :base_index: Base index
    :composite_size: Buckets per composite page
    :returns: List of revised ElasticSearch documents
    """

    documents = []
    index_pattern = f"{cluster_name}-{base_index}-*,-itgsoc-*_topic_unknown*"

    # Private copy: never mutate the module-level query
    aggs = copy.deepcopy(STATS_QUERY["aggs"])
    aggs["multiagg"]["composite"]["size"] = composite_size

    logging.debug(f'Getting stats for base index "{base_index}".')

    # Composite aggs return at most `size` buckets per call: page with after_key
    while True:
        result = _WORKER_ES.search(index=index_pattern, size=0, aggs=aggs, filter_path="-**.buckets.*.count")
        multiagg = result.body.get("aggregations", {}).get("multiagg", {})
        buckets = multiagg.get("buckets", [])

        for bucket in buckets:
            try:
                # Move the "key" values at the root of the document
                key = bucket.pop("key")
                bucket.update(key)

                # If the flow provider is None, set a default string
                if bucket["flowprovider"] is None:
                    bucket["flowprovider"] = "N/A"

                # >>> CHECK: lines ~204-211 weren't visible in your photos.
                # >>> Paste any other per-bucket processing you had here.

                documents.append(bucket)
            except (KeyError, TypeError) as e:
                logging.warning(f'Unexpected bucket for base index "{base_index}": {e}.')

        after_key = multiagg.get("after_key")
        if len(buckets) < composite_size or after_key is None:
            break
        aggs["multiagg"]["composite"]["after"] = after_key

    logging.debug(f'Got {len(documents)} documents for base index "{base_index}".')
    return documents


def send_documents(es, base_index, documents, cur_datetime):
    """Index the documents of one base index into the stats index.

    :returns: True on success, False otherwise
    """

    if not documents:
        return True

    actions = (
        {"_index": STATS_INDEX, "_source": {**document, "@timestamp": cur_datetime}}
        for document in documents
    )
    try:
        success, _ = helpers.bulk(es, actions)
        logging.info(f'Sent {success} documents for base index "{base_index}".')
        return True
    except helpers.BulkIndexError as e:
        logging.error(f'{len(e.errors)} documents rejected for base index "{base_index}": {e.errors[:3]}')
    except Exception:
        logging.exception(f'Failed to send documents for base index "{base_index}".')
    return False


def collect_and_send(es, args):
    """Get the stats of every base index in worker processes and send them.

    :returns: 1 in case of error, 0 otherwise
    """

    cluster_name = args["cluster_name"]

    # Get the base indices
    base_indices = get_base_indices(es, cluster_name)
    if not base_indices:
        logging.error("No base indices found (or failed to list them).")
        return 1

    cur_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    remaining = sorted(base_indices)
    failed = set()
    total_docs = 0

    # "spawn": workers start clean instead of inheriting the parent's SSL/heap state
    mp_context = multiprocessing.get_context("spawn")

    for attempt in range(1, args["max_attempts"] + 1):
        # Fewer workers on each retry: 5 -> 2 -> 1 with the defaults
        workers = max(1, args["workers"] >> (attempt - 1))
        logging.info(f"Attempt {attempt}/{args['max_attempts']}: {len(remaining)} base indices, {workers} workers.")
        crashed = []

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            mp_context=mp_context,
            initializer=init_worker,
            initargs=(args,),
        ) as executor:
            futures = {
                executor.submit(get_stats, cluster_name, base_index, args["composite_size"]): base_index
                for base_index in remaining
            }

            # Send each base index as soon as it's done instead of keeping everything in memory
            for future in concurrent.futures.as_completed(futures):
                base_index = futures[future]
                try:
                    documents = future.result()
                except BrokenProcessPool:
                    # A worker died (e.g. segfault): this task never finished, retry it
                    crashed.append(base_index)
                    continue
                except Exception:
                    logging.exception(f'Failed to get stats for base index "{base_index}".')
                    failed.add(base_index)
                    continue

                if send_documents(es, base_index, documents, cur_datetime):
                    total_docs += len(documents)
                else:
                    failed.add(base_index)

        if not crashed:
            remaining = []
            break

        logging.error(f"A worker process crashed during attempt {attempt}: {len(crashed)} base indices left to retry.")
        remaining = sorted(crashed)

    failed.update(remaining)

    logging.info(f"Sent {total_docs} documents to ElasticSearch.")
    if failed:
        logging.error(f"{len(failed)} base indices failed: {', '.join(sorted(failed))}")
        return 1
    return 0


def main():
    """Launch the script.

    :returns: 1 in case of error, 0 otherwise
    """

    # Parse the command line arguments
    args = get_prog_args()

    # Setup logging
    setup_logging(args["loglevel"])

    # Fewer glibc malloc arenas in the workers (they inherit this at start).
    # Set it in the Rundeck job environment too, so the parent gets it.
    os.environ.setdefault("MALLOC_ARENA_MAX", "2")

    # Client of the parent process: lists the indices and sends the results
    es = build_es(args)
    try:
        return collect_and_send(es, args)
    finally:
        es.close()


if __name__ == "__main__":
    sys.exit(main())


import threading

import urllib3.connection

_SSL_SETUP_LOCK = threading.Lock()
_original_ssl_wrap_socket = urllib3.connection.ssl_wrap_socket


def _locked_ssl_wrap_socket(*args, **kwargs):
    with _SSL_SETUP_LOCK:
        return _original_ssl_wrap_socket(*args, **kwargs)


if getattr(urllib3.connection.ssl_wrap_socket, "__name__", "") != "_locked_ssl_wrap_socket":
    urllib3.connection.ssl_wrap_socket = _locked_ssl_wrap_socket