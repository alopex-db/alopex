"""Exact value, type, and order oracles for the four frozen SQL operations."""
import hashlib
import json

ROWS = 34_250
ORIGINAL = [{"id": 1, "value": "old"}]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def expected(case):
    if case == "aggregate":
        return [{"count": 17_125}]
    if case == "ordered":
        return [{"id": i, "value": float(i)} for i in range(16)]
    if case == "select-all":
        return [{"id": i, "value": float(i)} for i in range(0, ROWS, 2)]
    if case == "insert-rollback":
        return [{"id": 3}]
    raise ValueError("CASE")


def exact(actual, wanted, code="ORACLE"):
    if type(actual) is not list or len(actual) != len(wanted):
        raise ValueError(code)
    for got, reference in zip(actual, wanted):
        if type(got) is not dict or set(got) != set(reference) or any(
                type(got[name]) is not type(value) or got[name] != value
                for name, value in reference.items()):
            raise ValueError(code)
    return digest(wanted)


def operation(case, result, rows):
    if rows != ROWS:
        raise ValueError("FIXTURE")
    return exact(result, expected(case))


def rollback(result):
    return exact(result, ORIGINAL, "ROLLBACK")


ORACLE_SHA256 = {case: digest(expected(case)) for case in
                 ("aggregate", "ordered", "select-all", "insert-rollback")}
ROLLBACK_SHA256 = digest(ORIGINAL)
