"""Tiny scalar fast-path contracts; no server, native build or timed workload."""

import math

import pytest
from alopex import AlopexError
from alopex.remote import _rows_to_dicts, _unwrap_sql_value


@pytest.mark.parametrize(
    "tag,payload,expected,expected_type",
    [
        ("Integer", -1, -1, int),
        ("BigInt", 2**60, 2**60, int),
        ("Timestamp", -123, -123, int),
        ("Text", "\x00雪", "\x00雪", str),
        ("Boolean", False, False, bool),
        ("Double", -0.0, -0.0, float),
        ("Double", 2, 2.0, float),
        ("Float", 0.1, 0.10000000149011612, float),
        ("Blob", [0, 255], b"\x00\xff", bytes),
        ("Vector", [0.1, 2], [0.10000000149011612, 2.0], list),
    ],
)
def test_tag_payload_keeps_exact_embedded_value_and_type(tag, payload, expected, expected_type):
    actual = _unwrap_sql_value({tag: payload})
    assert actual == expected
    assert type(actual) is expected_type
    if tag == "Double" and payload == 0:
        assert math.copysign(1, actual) == -1


@pytest.mark.parametrize(
    "wire",
    [
        {"Integer": True}, {"BigInt": False}, {"Timestamp": True},
        {"Integer": 1.0}, {"Boolean": 1}, {"Text": 1},
        {"Double": True}, {"Double": None}, {"Float": None},
        {"Double": "1.0"}, {"Float": True},
        {"Blob": [True]}, {"Blob": [256]}, {"Vector": [True]},
        {"Vector": [None]}, {"Unknown": 1}, {"Unknown": None},
        {}, {"Integer": 1, "Text": "second"}, "Unknown", 1,
    ],
)
def test_invalid_tag_shape_and_payload_remain_protocol_errors(wire):
    with pytest.raises(AlopexError) as raised:
        _unwrap_sql_value(wire)
    assert raised.value.code == "ALOPEX-PY203"


def test_each_row_value_is_validated_without_inferred_column_type():
    rows = [[{"Integer": 1}], ["Null"], [{"Text": "later"}], [{"Double": 2}]]
    result = _rows_to_dicts(["v"], rows)
    assert result == [{"v": 1}, {"v": None}, {"v": "later"}, {"v": 2.0}]
    assert [type(r["v"]) for r in result] == [int, type(None), str, float]
    assert rows == [[{"Integer": 1}], ["Null"], [{"Text": "later"}], [{"Double": 2}]]


def test_duplicate_column_does_not_hide_invalid_earlier_value():
    with pytest.raises(AlopexError) as raised:
        _rows_to_dicts(["v", "v"], [[{"Integer": True}, {"Text": "last"}]])
    assert raised.value.code == "ALOPEX-PY203"


def test_python_scalar_subclasses_keep_existing_fallback_contract():
    class IntValue(int):
        pass

    class TextValue(str):
        pass

    class DoubleValue(float):
        pass

    for tag, value in (("Integer", IntValue(1)), ("Text", TextValue("x"))):
        assert _unwrap_sql_value({tag: value}) is value
    result = _unwrap_sql_value({"Double": DoubleValue(1.5)})
    assert result == 1.5 and type(result) is float
