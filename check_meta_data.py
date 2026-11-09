"""Detect (and optionally strip) Elasticsearch metadata fields from a document.

Only top-level keys matter: a nested key like {"event": {"_id": 1}} is a normal
field (event._id). A dotted top-level key like "_id.foo" is checked on its first
segment, since Elasticsearch expands it into an object named "_id".
"""

# Rejected by the mapping layer ("is a metadata field and cannot be added inside a document")
MAPPING_METADATA = {
    "_id", "_index", "_routing", "_source", "_size",
    "_seq_no", "_primary_term", "_version",
    "_ignored", "_ignored_source", "_field_names", "_tier",
    "_data_stream_timestamp", "_tsid", "_nested_path", "_meta",
}

# Rejected by ingest pipelines ("unexpected metadata [...] in source")
INGEST_METADATA = {
    "_index", "_id", "_routing", "_version", "_version_type",
    "_if_seq_no", "_if_primary_term", "_dynamic_templates",
}

FORBIDDEN = MAPPING_METADATA | INGEST_METADATA

# Metadata field that IS allowed in the source (pre-aggregated docs)
ALLOWED = {"_doc_count"}


def find_metadata_fields(doc: dict) -> list[str]:
    """Return the top-level keys of `doc` that Elasticsearch will reject."""
    bad = []
    for key in doc:
        root = key.split(".", 1)[0]
        if root in FORBIDDEN and root not in ALLOWED:
            bad.append(key)
    return bad


def has_metadata_fields(doc: dict) -> bool:
    """True if `doc` contains at least one forbidden metadata field."""
    return bool(find_metadata_fields(doc))


def strip_metadata_fields(doc: dict) -> dict:
    """Return a copy of `doc` without forbidden metadata fields."""
    bad = set(find_metadata_fields(doc))
    return {k: v for k, v in doc.items() if k not in bad}


if __name__ == "__main__":
    sample = {
        "_id": "1",
        "_index": "op",
        "_doc_count": 5,
        "_id.extra": "x",
        "message": "hello",
        "event": {"_id": "nested-is-fine"},
    }
    print(find_metadata_fields(sample))   # ['_id', '_index', '_id.extra']
    print(has_metadata_fields(sample))    # True
    print(strip_metadata_fields(sample))  # keeps _doc_count, message, event