#!/usr/bin/env python3
"""Bounded public KV diagnosis, outside CI, reusing the serial supervisor."""
from run_sql_delete_profile import main
from kv_read_profile import OPERATIONS


if __name__ == "__main__":
    main(driver_name="kv_read_profile.py", row_choices=(3, 10000), default_rows=(10000,),
         operations=OPERATIONS, profile_operations=OPERATIONS, default_surfaces=("embedded",),
         scope="internal persisted-KV diagnosis; native sample includes result validation, cProfile only public operations; not reference comparison")
