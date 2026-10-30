#!/usr/bin/env python3
"""Bounded, serial, manual SQLite composite diagnosis."""
from run_sql_delete_profile import main
from sqlite_composite_profile import OPERATIONS

if __name__ == "__main__":
    main(driver_name="sqlite_composite_profile.py", row_choices=(3, 34250), default_rows=(3, 34250),
         operations=OPERATIONS, profile_operations=("alopex-checked", "sqlite-checked"),
         default_surfaces=("embedded",), worker_threads=1,
         scope="in-memory original composite and checked-result/FK-enforced diagnostic; not release qualification")
