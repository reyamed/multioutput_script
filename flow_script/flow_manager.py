from elasticsearch import Elasticsearch
import pandas as pd
import re


def get_cat_indices(
    es: Elasticsearch,
    output_file: str = "elasticsearch_indices.xlsx"
):
    """
    Get Elasticsearch _cat/indices information, aggregate by code_name,
    and generate an Excel file.

    Assumes index names contain the code_name as the first component
    separated by '-'.

    Example:
        abc-orders-prod     -> code_name = abc
        xyz-customers-prod  -> code_name = xyz
    """

    # Get _cat/indices with storage size in bytes
    indices = es.cat.indices(
        format="json",
        bytes="b",
        h="index,pri,store.size"
    )

    result = {}

    for index in indices:
        index_name = index.get("index")

        if not index_name:
            continue

        # ---------------------------------------------------------
        # Extract code_name from the index name
        # Change this logic if your naming convention is different.
        # ---------------------------------------------------------
        code_name = index_name.split("-")[0]

        # Number of primary shards
        shards = int(index.get("pri", 0))

        # Storage in bytes
        storage_bytes = int(index.get("store.size", 0))

        # Initialize code_name
        if code_name not in result:
            result[code_name] = {
                "number_of_shards": 0,
                "storage_gb": 0.0
            }

        # Aggregate
        result[code_name]["number_of_shards"] += shards
        result[code_name]["storage_gb"] += storage_bytes / (1024 ** 3)

    # Round storage
    for code_name in result:
        result[code_name]["storage_gb"] = round(
            result[code_name]["storage_gb"], 2
        )

    # Create DataFrame
    df = pd.DataFrame(
        [
            {
                "code_name": code_name,
                "number_of_shards": values["number_of_shards"],
                "storage_gb": values["storage_gb"]
            }
            for code_name, values in result.items()
        ]
    )

    # Sort by code name
    df = df.sort_values("code_name").reset_index(drop=True)

    # Generate Excel
    df.to_excel(output_file, index=False)

    return result, df