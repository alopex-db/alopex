"""Version-neutral replacement for the old same-process diagnostic prose check."""


def test_second_open_keeps_exception_code_refusal_and_release_contract(tmp_path):
    import alopex
    import pytest

    path = str(tmp_path / "locked.alopex")
    first = alopex.Database.open(path)
    try:
        with pytest.raises(alopex.AlopexError) as raised:
            alopex.Database.open(path)
        assert raised.value.code == "ALOPEX-PY999"
        assert "already open" in str(raised.value)
        assert "lock file:" in str(raised.value)
    finally:
        first.close()
    reopened = alopex.Database.open(path)
    reopened.close()
