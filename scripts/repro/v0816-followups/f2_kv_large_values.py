"""Bounded #678 reproduction: three two-byte values, four-byte page budget."""
from alopex import Database, TxnMode


def main():
    db = Database.new()
    expected = [(b"p0", b"vv"), (b"p1", b"vv"), (b"p2", b"vv")]
    with db.begin(TxnMode.READ_WRITE) as txn:
        for key, value in expected:
            txn.put(key, value)
        for kind, args in [("prefix", (b"p",)), ("range", (b"p", b"q"))]:
            assert list(getattr(txn, f"scan_{kind}")(*args, max_bytes=4)) == expected
            cursor = None
            actual = []
            for _ in range(3):
                page = getattr(txn, f"scan_{kind}_page")(*args, max_bytes=4, cursor=cursor)
                actual.extend(page["entries"])
                if not page["has_more"]:
                    break
                cursor = page["next_cursor"]
            assert actual == expected
    db.close()
    print("#678: prefix/range complete scans and byte-bound pages passed (3 entries)")


if __name__ == "__main__":
    main()
