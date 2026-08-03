# -*- coding: utf-8 -*-
# https://excalidraw.com/#json=GcwKgg_oFcdtu7VeqFbpC,mY-hPMLVPE42n-ULAKYHlA
"""Elasticsearch bulk sender with a dynamically retargetable cluster.

The Elasticsearch hosts are no longer read from "config.yml": they are
assigned to a Streamer group in Etcd and delivered in the dynamic
configuration event. This module therefore separates two concerns that used
to be merged in the constructor:

    * _ElasticClientFactory holds everything that is fixed for the lifetime
      of the process (SSL, certificates, timeouts) and builds a client for a
      given set of hosts.
    * TrainSender owns the current client and swaps it at a safe point, i.e.
      when no bulk request is in flight.

Because the swap only happens when no bulk is running, no lock is needed:
the "no bulk in flight" invariant replaces mutual exclusion, and it also
makes closing the previous client immediately safe.

    !!! ------------------------------------------------------------------
    !!! Two regions below are marked "PASTE FROM YOUR VERSION". They hold
    !!! code that was not changed by this refactor and that is not
    !!! reproduced here. They raise NotImplementedError on purpose so the
    !!! omission fails loudly instead of silently misbehaving.
    !!! ------------------------------------------------------------------
"""

import logging

import zmq
from elasticsearch import Elasticsearch


class ClusterMismatchError(Exception):
    """Raised when the cluster reached is not the expected one."""


class _ElasticClientFactory(object):
    """Build Elasticsearch clients for an arbitrary set of hosts."""

    def __init__(self, config):
        """Constructor of the object"""

        # Disable loggers of libraries
        for logger_name in ("elasticsearch", "urllib3"):
            logger = logging.getLogger(logger_name)
            logger.addHandler(logging.NullHandler())
            logger.propagate = False

        # Set default ElasticSearch configuration. Everything except the
        # hosts is fixed for the lifetime of this process.
        self._host_scheme = "https://" if config["elasticsearch_use_ssl"] else "http://"
        self._base_config = {
            "maxsize": 1,
            "ssl_show_warn": False,
            "timeout": config["elasticsearch_request_timeout"],
        }

        # Set SSL-specific configuration
        if config["elasticsearch_use_ssl"]:

            # ----------------------------------------------------------------
            # PASTE FROM YOUR VERSION (1/2)
            #
            # Your original SSL block wrote its keys straight into
            # self._elastic_config. Write them into self._base_config
            # instead; nothing else about it changes. Typically:
            #
            #   self._base_config.update({
            #       "use_ssl": True,
            #       "verify_certs": config["elasticsearch_verify_certs"],
            #       "ca_certs": config["elasticsearch_ca_certs"],
            #       "client_cert": config["elasticsearch_client_cert"],
            #       "client_key": config["elasticsearch_client_key"],
            #   })
            # ----------------------------------------------------------------
            raise NotImplementedError(
                "Paste your original SSL configuration block here, writing "
                "into self._base_config."
            )

    def create_client(self, hosts):
        """Create a client pointed at the given hosts.

        :hosts: List of "host:port" strings
        :returns: An Elasticsearch client
        """

        elastic_config = dict(self._base_config)
        elastic_config["hosts"] = [
            "{}{}".format(self._host_scheme, host) for host in hosts
        ]

        return Elasticsearch(**elastic_config)


class TrainSender(object):
    """Send bulks to the Elasticsearch cluster assigned to our group."""

    def __init__(self, config, context):
        """Constructor of the object"""

        # Create the Elasticsearch Factory
        self._es_factory = _ElasticClientFactory(config)

        # The client is built from the dynamic configuration, either the one
        # inherited at spawn time or the one received on the events bus.
        self._es_client = None
        self._es_hosts = []
        self._es_cluster_name = None
        self._es_fingerprint = None
        self._pending_es = None

        # Create the signal socket
        self.signal_socket = context.socket(zmq.PULL)
        self.signal_socket.bind("inproc://trainsender")

        # Class variables
        self._config = config
        self._context = context
        self._executors = []
        self._max_bulks = 0

        # In Etcd mode the main process merges the configuration before
        # spawning this component, so the cluster is usually already known.
        self.apply_es_config(config.get("elasticsearch"))
        self.maybe_switch_cluster()

    @property
    def is_ready(self):
        """Whether an Elasticsearch client is available."""

        return self._es_client is not None

    @property
    def es_client(self):
        """The Elasticsearch client currently in use.

        Always read through this property at the point of use: keeping a
        local reference across a swap pins the caller to the old cluster.
        """

        return self._es_client

    @property
    def cluster_name(self):
        """Name of the cluster currently in use, or None."""

        return self._es_cluster_name

    def apply_es_config(self, es_section):
        """Stage a new Elasticsearch target.

        The change is applied later, at the next safe point.

        :es_section: The "elasticsearch" section of a configuration event
        :returns: Nothing
        """

        if es_section is None:
            return

        # The configuration event is re-sent on any change, not only on a
        # cluster move: ignore the ones that do not concern us.
        if es_section.get("fingerprint") == self._es_fingerprint:
            return

        logging.info(
            'A switch to cluster "{}" is pending.'.format(
                es_section.get("cluster_name")
            )
        )
        self._pending_es = es_section

    def maybe_switch_cluster(self):
        """Swap the Elasticsearch client if a change is pending.

        The swap is only performed when no bulk is in flight, which makes it
        safe to close the previous client straight away.

        :returns: Nothing
        """

        if self._pending_es is None:
            return

        # Bulks are still running: retry on the next iteration
        if self._executors:
            return

        new_config = self._pending_es
        old_client = self._es_client

        try:

            # Validate before committing: an unreachable or unexpected
            # cluster must not interrupt ingestion on the current one.
            client = self._es_factory.create_client(new_config["hosts"])
            self._check_cluster_name(new_config["cluster_name"], client)

        except Exception:
            logging.exception(
                'Rejected the switch to cluster "{}" ({}). Staying on "{}".'.format(
                    new_config.get("cluster_name"),
                    new_config.get("hosts"),
                    self._es_cluster_name,
                )
            )

            # Dropped on purpose: keeping it would make every iteration pay
            # a full request timeout against an unreachable cluster.
            self._pending_es = None
            return

        self._es_client = client
        self._es_hosts = list(new_config["hosts"])
        self._es_cluster_name = new_config["cluster_name"]
        self._es_fingerprint = new_config.get("fingerprint")
        self._pending_es = None

        if old_client is not None:
            try:
                old_client.close()
            except Exception:
                logging.exception("Failed to close the previous Elasticsearch client.")

        logging.info(
            'Elasticsearch client switched to "{}" ({}).'.format(
                self._es_cluster_name, self._es_hosts
            )
        )

    def drop_cluster(self):
        """Release the client, for instance when the group is unassigned.

        :returns: Nothing
        """

        if self._es_client is None:
            return

        if self._executors:
            logging.warning(
                "Bulks are still in flight, the Elasticsearch client will be "
                "released at the next safe point."
            )
            return

        try:
            self._es_client.close()
        except Exception:
            logging.exception("Failed to close the Elasticsearch client.")

        self._es_client = None
        self._es_hosts = []
        self._es_cluster_name = None
        self._es_fingerprint = None
        logging.warning("The Elasticsearch client was released.")

    def _check_cluster_name(self, expected_name, es_client):
        """Make sure the cluster we connect to is the expected one.

        The expected name now comes from the group assignment in Etcd
        instead of the static configuration.

        :expected_name: Name the cluster is supposed to report
        :es_client: Client to check
        :returns: Nothing
        :raises ClusterMismatchError: If the names differ
        """

        actual_name = es_client.info()["cluster_name"]

        if actual_name != expected_name:
            raise ClusterMismatchError(
                'Connected to cluster "{}" but "{}" was expected.'.format(
                    actual_name, expected_name
                )
            )

    # --------------------------------------------------------------------
    # PASTE FROM YOUR VERSION (2/2)
    #
    # Everything below was not touched by this refactor and is not
    # reproduced here: bulk building, the executors pool, flush, the
    # rework flow, metrics, close/stop. Two rules when pasting it back:
    #
    #   1. Read the client through self.es_client at the point of use,
    #      never into a local kept across iterations.
    #   2. Guard the send path with "if not self.is_ready: return" so a
    #      Streamer without a cluster buffers instead of crashing.
    # --------------------------------------------------------------------

    def reap_executors(self):
        """Collect the finished bulk executors."""

        raise NotImplementedError("Paste your original implementation here.")

    def flush(self):
        """Send the buffered documents to Elasticsearch."""

        raise NotImplementedError("Paste your original implementation here.")
