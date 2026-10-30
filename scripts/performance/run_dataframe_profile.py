#!/usr/bin/env python3
"""Bounded DataFrame phase diagnosis, separate from ordinary CI."""
from run_sql_delete_profile import main
from dataframe_profile import OPERATIONS


if __name__ == "__main__":
    main(driver_name="dataframe_profile.py", row_choices=(3, 34250),
         default_rows=(3, 34250), operations=OPERATIONS, profile_operations=OPERATIONS,
         default_surfaces=("embedded",), worker_threads=1,
         scope="current-source DataFrame phase diagnosis; copied pinned original workload; not published/reference comparison")
