#!/usr/bin/env python3
"""Manual, serial diagnosis of the four original #712 SQL cases; forbidden in CI."""
from run_sql_delete_profile import main
from sql_query_profile import OPERATIONS, fixture_rows


if __name__ == "__main__":
    main(driver_name="sql_query_profile.py", row_choices=(3, 34250), default_rows=(34250,),
         operations=OPERATIONS, profile_operations=OPERATIONS, row_count=fixture_rows,
         scope="internal diagnosis; native stacks include validation, client cProfile covers public operations only; not a reference comparison")
