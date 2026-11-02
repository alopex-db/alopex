#!/usr/bin/env python3
"""Bounded serial manual plain CSV stream diagnosis."""
from run_sql_delete_profile import main
from csv_stream_profile import OPERATIONS

if __name__ == "__main__":
    main(driver_name="csv_stream_profile.py", row_choices=(3, 34250), default_rows=(3, 34250),
         operations=OPERATIONS, profile_operations=OPERATIONS, default_surfaces=("embedded",),
         worker_threads=1, scope="original plain CSV full stream consumption; current source/reference diagnosis")
