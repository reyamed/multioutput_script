# -*- coding: utf-8 -*-
"""Tests of the cluster assignment validation."""

import pytest

from bnpp_streamer.streamer.libraries.cluster_assignment import (
    ClusterAssignmentError,
    build_es_section,
    cluster_fingerprint,
    validate_cluster_assignment,
)


VALID = {"cluster_name": "es-soc-dc1", "hosts": ["es02:9200", "es01:9200"]}


def test_valid_assignment_is_normalised():
    result = validate_cluster_assignment(VALID)

    assert result["cluster_name"] == "es-soc-dc1"
    assert result["hosts"] == ["es01:9200", "es02:9200"]


def test_duplicated_hosts_are_removed():
    result = validate_cluster_assignment(
        {"cluster_name": "es", "hosts": ["es01:9200", "es01:9200"]}
    )

    assert result["hosts"] == ["es01:9200"]


@pytest.mark.parametrize(
    "cluster",
    [
        [],
        {"hosts": ["es01:9200"]},
        {"cluster_name": "es"},
        {"cluster_name": "", "hosts": ["es01:9200"]},
        {"cluster_name": "es", "hosts": []},
        {"cluster_name": "es", "hosts": "es01:9200"},
        {"cluster_name": "es", "hosts": ["es01"]},
        {"cluster_name": "es", "hosts": ["es01:abc"]},
        {"cluster_name": "es", "hosts": ["es01:99999"]},
        {"cluster_name": "es", "hosts": [":9200"]},
    ],
)
def test_invalid_assignments_are_rejected(cluster):
    with pytest.raises(ClusterAssignmentError):
        validate_cluster_assignment(cluster)


def test_fingerprint_ignores_host_order():
    a = validate_cluster_assignment(
        {"cluster_name": "es", "hosts": ["es01:9200", "es02:9200"]}
    )
    b = validate_cluster_assignment(
        {"cluster_name": "es", "hosts": ["es02:9200", "es01:9200"]}
    )

    assert cluster_fingerprint(a) == cluster_fingerprint(b)


def test_fingerprint_changes_with_the_cluster_name():
    a = validate_cluster_assignment({"cluster_name": "es-a", "hosts": ["es01:9200"]})
    b = validate_cluster_assignment({"cluster_name": "es-b", "hosts": ["es01:9200"]})

    assert cluster_fingerprint(a) != cluster_fingerprint(b)


def test_fingerprint_changes_when_a_host_is_added():
    a = validate_cluster_assignment({"cluster_name": "es", "hosts": ["es01:9200"]})
    b = validate_cluster_assignment(
        {"cluster_name": "es", "hosts": ["es01:9200", "es02:9200"]}
    )

    assert cluster_fingerprint(a) != cluster_fingerprint(b)


def test_es_section_shape():
    section = build_es_section(validate_cluster_assignment(VALID))

    assert set(section) == {"cluster_name", "hosts", "fingerprint"}
    assert section["hosts"] == ["es01:9200", "es02:9200"]
