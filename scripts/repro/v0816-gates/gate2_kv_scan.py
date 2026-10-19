"""Gate 2 (#670): Transaction.scan_prefix / scan_range silently truncate at the default
page size and expose no cursor; exactly `scan_budget` candidates raises.

Exit code: 1 = defect reproduced, 0 = fixed.
"""
import sys
import tempfile

from alopex import Database, TxnMode

db = Database.open(tempfile.mkdtemp() + "/gate2.alopex")
tx = db.begin(TxnMode.READ_WRITE)
for i in range(250):
    tx.put(b"cart:c1:%04d" % i, b"1")
tx.commit()
del tx

tx = db.begin(TxnMode.READ_ONLY)
problems = []

it = tx.scan_prefix(b"cart:c1:")
print("scan_prefix returns:", type(it).__name__, "public attrs:", [a for a in dir(it) if not a.startswith("_")])
n_prefix = len(list(it))
n_range = len(list(tx.scan_range(b"cart:c1:", b"cart:c1:\xff")))
print("len(list(scan_prefix)) =", n_prefix, "(250 keys stored)")
print("len(list(scan_range))  =", n_range, "(250 keys stored)")
if n_prefix != 250:
    problems.append(f"scan_prefix returned {n_prefix} of 250 without error")
if n_range != 250:
    problems.append(f"scan_range returned {n_range} of 250 without error")

try:
    n = len(list(tx.scan_prefix(b"cart:c1:", limit=None)))
    print("scan_prefix(limit=None) =", n)
except Exception as e:  # noqa: BLE001
    print("scan_prefix(limit=None) ERROR:", type(e).__name__, e)
    problems.append("limit=None is not accepted")

try:
    n = len(list(tx.scan_prefix(b"cart:c1:", scan_budget=250, limit=1000)))
    print("scan_prefix(scan_budget=250) with exactly 250 keys =", n)
except Exception as e:  # noqa: BLE001
    print("scan_prefix(scan_budget=250) with exactly 250 keys ERROR:", e)
    problems.append("exactly scan_budget candidates raises")

for p in problems:
    print("  -> DEFECT:", p)
print("RESULT:", "REPRODUCED (#670)" if problems else "not reproduced / fixed")
sys.exit(1 if problems else 0)
