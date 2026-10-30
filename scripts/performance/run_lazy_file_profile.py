#!/usr/bin/env python3
"""Bounded manual lazy-file checks, separate from ordinary CI."""
from run_sql_delete_profile import main
from lazy_file_profile import OPERATIONS

if __name__ == "__main__":
    main(driver_name="lazy_file_profile.py", row_choices=(3, 34250),
         default_rows=(3, 34250), operations=OPERATIONS,
         profile_operations=("csv-collect", "csv-batches"),
         default_surfaces=("embedded",), worker_threads=1,
         scope="fixed current-source CSV expressions/numeric Parquet guards; not reference qualification")
