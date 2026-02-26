"""Hashing utilities for data lineage and checksums."""

import hashlib
from pathlib import Path


def file_sha256(path: Path, chunk_size: int = 8192) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def dataframe_hash(df, columns: list = None) -> str:
    """Compute a hash of a DataFrame's contents for versioning."""
    import pandas as pd
    if columns:
        df = df[columns]
    return hashlib.sha256(
        pd.util.hash_pandas_object(df).values.tobytes()
    ).hexdigest()[:16]
