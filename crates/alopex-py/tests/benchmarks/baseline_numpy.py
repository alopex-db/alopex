from __future__ import annotations

from typing import Any


def cosine_similarity_loop(query: Any, vectors: Any):
    """Cosine similarity baseline using a pure Python loop.

    Restrictions (NFR-1):
      - Use numpy.dot + numpy.linalg.norm inside the for-loop.
    """
    import numpy as np

    q = np.asarray(query, dtype=np.float32)
    v = np.asarray(vectors, dtype=np.float32)
    if v.ndim != 2:
        raise ValueError("vectors must be a 2D array")
    if q.ndim != 1:
        raise ValueError("query must be a 1D array")
    if v.shape[1] != q.shape[0]:
        raise ValueError("dimension mismatch")

    q_norm = float(np.linalg.norm(q))
    out = np.empty((v.shape[0],), dtype=np.float32)

    denom_q = q_norm if q_norm != 0.0 else 1.0
    for i in range(v.shape[0]):
        dot = float(np.dot(q, v[i]))
        v_norm = float(np.linalg.norm(v[i]))
        denom = denom_q * (v_norm if v_norm != 0.0 else 1.0)
        out[i] = dot / denom

    return out


def cosine_similarity_vectorized(query: Any, vectors: Any):
    """Reference cosine similarity via vectorized NumPy operations."""
    import numpy as np

    q = np.asarray(query, dtype=np.float32)
    v = np.asarray(vectors, dtype=np.float32)
    if v.ndim != 2:
        raise ValueError("vectors must be a 2D array")
    if q.ndim != 1:
        raise ValueError("query must be a 1D array")
    if v.shape[1] != q.shape[0]:
        raise ValueError("dimension mismatch")

    denom = np.linalg.norm(q) * np.linalg.norm(v, axis=1)
    denom = np.where(denom == 0.0, 1.0, denom)
    return np.dot(q, v.T) / denom


def cosine_similarity_loop_over_get_vector(txn: Any, keys: list[bytes], query: Any, metric: Any):
    """Cosine similarity baseline using a Python loop + per-key get_vector.

    This represents a naive Python approach:
      - iterate keys in Python
      - fetch each vector via the bindings (N calls)
      - compute cosine similarity with numpy.dot + numpy.linalg.norm in the loop
    """
    import numpy as np

    q = np.asarray(query, dtype=np.float32)
    if q.ndim != 1:
        raise ValueError("query must be a 1D array")

    q_norm = float(np.linalg.norm(q))
    denom_q = q_norm if q_norm != 0.0 else 1.0

    out = np.empty((len(keys),), dtype=np.float32)
    for i, key in enumerate(keys):
        v = txn.get_vector(key, metric)
        dot = float(np.dot(q, v))
        v_norm = float(np.linalg.norm(v))
        denom = denom_q * (v_norm if v_norm != 0.0 else 1.0)
        out[i] = dot / denom

    return out
