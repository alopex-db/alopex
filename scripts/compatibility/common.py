"""Deterministic local regression inputs; not external DB conformance cases."""

import numpy as np


def seed(dim):
    notes = {1: "", 2: "日本語,'quote'\nnext", 3: "NULL vector", 4: None, 5: "distinct"}
    result = []
    for row_id in range(1, 6):
        if row_id == 3:
            vector = None
        elif row_id == 5:
            vector = (
                np.random.default_rng(1337 + dim)
                .standard_normal(dim)
                .astype(np.float32)
                .tolist()
            )
        else:
            vector = (
                np.arange(dim, dtype=np.float32) * np.float32(0.03125)
                + np.float32(row_id * 0.125)
            ).tolist()
            vector[:3] = [
                float(np.float32(row_id * 0.1)),
                -row_id * 0.25,
                float(np.float32(row_id / 3)),
            ]
        result.append({"id": row_id, "embedding": vector, "note": notes[row_id]})
    return result
