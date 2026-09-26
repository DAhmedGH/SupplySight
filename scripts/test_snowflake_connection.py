"""Compatibility launcher for the read-only Snowflake development check."""

from supplysight.snowflake_check import main

if __name__ == "__main__":
    raise SystemExit(main())
