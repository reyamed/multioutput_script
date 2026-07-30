# -*- coding: utf-8 -*-
"""Elasticsearch cluster assignment support for the EtcdWatcher.

The logic lives in a mixin so that "etcdwatcher.py" only needs a handful of
new lines instead of being restructured. Add the mixin to the bases of
EtcdWatcher::

    class EtcdWatcher(ClusterAssignmentMixin, StreamerComponent):
        ...

The mixin expects the following attributes to exist on the instance, which
they already do:

    * self._etcd            The Etcd client, or None when disconnected
    * self._assigned_group  The group this Streamer is assigned to, or None
    * self.config           The static configuration
"""

import logging

from bnpp_streamer.streamer.libraries.cluster_assignment import (
    ClusterAssignmentError,
    build_es_section,
    validate_cluster_assignment,
)

# Etcd prefix holding one key per group
CLUSTER_ASSIGNMENT_PREFIX = "/cluster_assignment"

# Name of the watch, used as a key in self._watches
CLUSTER_ASSIGNMENT_WATCH = "cluster_assignment"


class ClusterAssignmentMixin(object):
    """Resolve the Elasticsearch cluster assigned to the current group."""

    def cluster_assignment_key(self, group=None):
        """Build the Etcd key holding the cluster of a group.

        :group: Group to build the key for, defaults to the assigned group
        :returns: The namespace-relative Etcd key
        """

        return "{}/{}".format(CLUSTER_ASSIGNMENT_PREFIX, group or self._assigned_group)

    def is_our_cluster_key(self, key):
        """Tell whether an Etcd key holds the cluster of our own group.

        The watch is set on a prefix, so it fires for every group. Without
        this check the client would be rebuilt on any other group's rollout.

        :key: Namespace-relative key reported by the watch callback
        :returns: True if the key belongs to the assigned group
        """

        if self._assigned_group is None:
            return False

        return key == self.cluster_assignment_key()

    def get_assigned_cluster(self):
        """Read and validate the cluster assigned to our group.

        :returns: A validated cluster assignment, or None
        """

        if self._etcd is None or self._assigned_group is None:
            return None

        key = self.cluster_assignment_key()
        cluster = self._etcd.get_json(key)

        if cluster is None:
            logging.error(
                'No Elasticsearch cluster is assigned to group "{}".'.format(
                    self._assigned_group
                )
            )
            return None

        try:
            return validate_cluster_assignment(cluster)

        except ClusterAssignmentError as e:
            logging.error(
                'Invalid cluster assignment for group "{}": {}'.format(
                    self._assigned_group, str(e)
                )
            )
            return None

    def get_es_section(self):
        """Build the "elasticsearch" section to embed in the config event.

        :returns: A dict to merge into the dynamic configuration, or None
        """

        cluster = self.get_assigned_cluster()

        if cluster is None:
            return None

        return build_es_section(cluster)
