#!/usr/bin/env python3
"""Manual SQL stream comparison with bounded serialized workers."""
from run_sql_delete_profile import main
from sql_stream_profile import OPERATIONS

if __name__ == "__main__":
    main(driver_name="sql_stream_profile.py", row_choices=(3, 34250),
         default_rows=(3, 34250), operations=OPERATIONS, profile_operations=OPERATIONS,
         default_surfaces=("embedded",), worker_threads=1,
         scope="SQL stream original count boundary and equal Python-row boundary; current source/reference")
