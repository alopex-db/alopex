"""KV scans with the default page size fail once one page would exceed max_bytes (follow-up of #674).

Exit 1 if reproduced, 0 if fixed.
"""
import sys
import tempfile

from alopex import Database, TxnMode

N, SIZE = 100, 200_000  # 100 x 200 KB = about 20 MB, over the default max_bytes (16 MiB)
db = Database.open(tempfile.mkdtemp() + "/a.alopex")
tx = db.begin(TxnMode.READ_WRITE)
for i in range(N):
    tx.put(b"big/%03d" % i, b"x" * SIZE)
tx.commit()
tx = db.begin(TxnMode.READ_ONLY)

failed = False


def check(label, fn):
    global failed
    try:
        r = fn()
        print(f"OK  {label} -> {r}", flush=True)
        return r
    except Exception as e:  # noqa: BLE001
        print(f"ERR {label} -> {type(e).__name__}: {str(e)[:160]}", flush=True)
        failed = True


check("len(list(scan_prefix(prefix)))  [defaults]", lambda: len(list(tx.scan_prefix(b"big/"))))
check("len(list(scan_range(start, end)))  [defaults]", lambda: len(list(tx.scan_range(b"big/", b"big0"))))


def page_through():
    got, cursor, pages = 0, None, 0
    while True:
        p = tx.scan_prefix_page(b"big/", cursor=cursor)  # default limit=100
        got += len(p["entries"])
        pages += 1
        if not p["has_more"]:
            return f"{got} entries in {pages} pages"
        cursor = p["next_cursor"]


check("scan_prefix_page() loop with defaults", page_through)
try:
    print("reference: scan_prefix(page_size=50) ->", len(list(tx.scan_prefix(b"big/", page_size=50))))
except TypeError as e:
    print("reference: page_size not supported by this build:", e)
print("RESULT:", "REPRODUCED" if failed else "OK")
sys.exit(1 if failed else 0)
