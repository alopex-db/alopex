import pytest
from alopex import AlopexError, DataFrame, LazyFrame


@pytest.mark.parametrize("row_count", [1023, 1024, 1025, 34_250])
def test_scan_csv_collect_to_dict_preserves_all_arrow_batches(tmp_path, row_count):
    path = tmp_path / f"rows-{row_count}.csv"
    path.write_text(
        "id,label\n"
        + "".join(f"{index},row-{index}\n" for index in range(row_count)),
        encoding="utf-8",
    )

    frame = LazyFrame.scan_csv(str(path)).collect()
    values = frame.to_dict()

    assert frame.height() == row_count
    assert values["id"] == list(range(row_count))
    assert values["label"] == [f"row-{index}" for index in range(row_count)]


def test_scan_csv_stream_is_publicly_iterable_across_batches(tmp_path):
    path = tmp_path / "stream.csv"
    path.write_text(
        "id\n" + "".join(f"{index}\n" for index in range(1025)),
        encoding="utf-8",
    )

    with LazyFrame.scan_csv(str(path)).collect_batches(chunk_size=1024) as batches:
        values = [
            value for batch in batches for value in batch.to_dict()["id"]
        ]

    assert values == list(range(1025))

    with pytest.raises(TypeError):
        LazyFrame.scan_csv(str(path)).collect(streaming=True)


def test_python_string_namespace_operations():
    df = DataFrame({"name": [" Alopex ", None, "Straße42"]})

    out = (
        df.str("name")
        .to_lowercase("lower")
        .str("lower")
        .replace(r"\d+", "#", "masked")
        .str("name")
        .contains(r"\d+$", "has_digits")
        .str("name")
        .len_chars("chars")
        .to_dict()
    )

    assert out["lower"] == [" alopex ", None, "straße42"]
    assert out["masked"] == [" alopex ", None, "straße#"]
    assert out["has_digits"] == [False, None, True]
    assert out["chars"] == [8, None, 8]


def test_python_datetime_namespace_operations():
    df = DataFrame(
        {"ts": [0, 1_704_067_200_123_000, None]},
        schema={"ts": "timestamp_micros"},
    )

    out = (
        df.dt("ts")
        .year("year")
        .dt("ts")
        .weekday("weekday")
        .dt("ts")
        .to_string("text")
        .dt("ts")
        .convert_time_zone("Z", "+09:00", "tokyo")
        .to_dict()
    )

    assert out["year"] == [1970, 2024, None]
    assert out["weekday"] == [4, 1, None]
    assert out["text"] == [
        "1970-01-01T00:00:00Z",
        "2024-01-01T00:00:00.123Z",
        None,
    ]
    assert out["tokyo"] == [32_400_000_000, 1_704_099_600_123_000, None]


def test_python_list_namespace_and_explode_implode_parity():
    df = DataFrame({"tags": ["db,rust", None, "db,"]})
    out = (
        df.str("tags")
        .split(",", "parts")
        .list("parts")
        .join("|", "NULL", "joined")
        .list("parts")
        .len("len")
        .list("parts")
        .contains("db", "has_db")
        .to_dict()
    )

    assert out["parts"] == [["db", "rust"], None, ["db", ""]]
    assert out["joined"] == ["db|rust", None, "db|"]
    assert out["len"] == [2, None, 2]
    assert out["has_db"] == [True, None, True]

    words = DataFrame({"word": ["a", None, "c"]})
    assert words.implode().explode("word").to_dict()["word"] == ["a", None, "c"]

    lists = DataFrame(
        {
            "id": ["x", "y", "z"],
            "items": [["a", "b"], [], None],
        },
        schema={"items": "list_utf8"},
    )
    exploded = lists.explode("items").to_dict()
    assert exploded["id"] == ["x", "x", "y", "z"]
    assert exploded["items"] == ["a", "b", None, None]


def test_python_dataframe_p3_errors_are_clear():
    df = DataFrame({"name": ["alopex"]})

    with pytest.raises(AlopexError) as invalid_regex:
        df.str("name").contains("[", "bad")
    assert "regex" in str(invalid_regex.value).lower()

    with pytest.raises(AlopexError) as invalid_type:
        df.dt("name").year("year")
    assert "timestamp" in str(invalid_type.value).lower()
