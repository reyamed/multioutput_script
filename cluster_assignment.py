# -*- coding: utf-8 -*-
"""Validation of the Elasticsearch cluster assigned to a Streamer group.

The cluster travels with the group assignment: the group a Streamer is
assigned to determines which Elasticsearch cluster it writes to. This module
is the last line of defence between an Etcd write and a live Producer, so it
is deliberately strict.

Etcd layout::

    /assignments               -> [{"streamer": "<id>", "group": "<group>"}]
    /configs/<group>           -> { ...processor configuration... }
    /cluster_assignment/<group> -> {"cluster_name": "...", "hosts": [...]}

Write "/configs/<group>" and "/cluster_assignment/<group>" in a single Etcd
transaction so that no watcher can ever observe them disagreeing.
"""

import hashlib

import orjson


class ClusterAssignmentError(Exception):
    """Raised when a cluster assignment read from Etcd is unusable."""


def validate_cluster_assignment(cluster):
    """Validate the Elasticsearch cluster assigned to a group.

    :cluster: Raw value read from /cluster_assignment/<group>
    :returns: A normalised dict with "cluster_name" and "hosts"
    :raises ClusterAssignmentError: If the value cannot be used
    """

    if not isinstance(cluster, dict):
        raise ClusterAssignmentError("The cluster assignment must be an object.")

    for key in ("cluster_name", "hosts"):
        if key not in cluster:
            raise ClusterAssignmentError(
                'The cluster assignment is missing the "{}" key.'.format(key)
            )

    cluster_name = cluster["cluster_name"]
    if not isinstance(cluster_name, str) or not cluster_name.strip():
        raise ClusterAssignmentError('"cluster_name" must be a non-empty string.')

    hosts = cluster["hosts"]
    if not isinstance(hosts, list) or not hosts:
        raise ClusterAssignmentError('"hosts" must be a non-empty list.')

    valid_hosts = []
    for host in hosts:

        if not isinstance(host, str) or ":" not in host:
            raise ClusterAssignmentError(
                'Invalid host "{}": the "host:port" format is expected.'.format(host)
            )

        hostname, _, port = host.rpartition(":")

        if not hostname.strip():
            raise ClusterAssignmentError(
                'Invalid host "{}": the hostname is empty.'.format(host)
            )

        # A non-numeric port silently produces an unreachable client, so it is
        # rejected here rather than at connection time.
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ClusterAssignmentError(
                'Invalid host "{}": "{}" is not a valid port.'.format(host, port)
            )

        valid_hosts.append(host.strip())

    # Duplicated hosts inflate the connection pool for no reason
    deduplicated = sorted(set(valid_hosts))

    return {"cluster_name": cluster_name.strip(), "hosts": deduplicated}


def cluster_fingerprint(cluster):
    """Build a stable identifier for a validated cluster assignment.

    Components compare fingerprints to tell a real change from a re-delivery:
    the configuration event is re-sent on any processor change, not only when
    the cluster moves.

    :cluster: A validated cluster assignment
    :returns: A short hexadecimal digest
    """

    payload = orjson.dumps(
        {
            "cluster_name": cluster["cluster_name"],
            "hosts": sorted(cluster["hosts"]),
        }
    )
    return hashlib.sha256(payload).hexdigest()[:16]


def build_es_section(cluster):
    """Build the "elasticsearch" section of a dynamic configuration event.

    :cluster: A validated cluster assignment
    :returns: A dict ready to be embedded in the configuration payload
    """

    return {
        "cluster_name": cluster["cluster_name"],
        "hosts": list(cluster["hosts"]),
        "fingerprint": cluster_fingerprint(cluster),
    }
