"""Read-only Snowflake development connectivity check."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from supplysight.settings import ConfigurationError, SnowflakeSettings

LOGGER = logging.getLogger("supplysight.snowflake_check")
CONTEXT_QUERY = """\
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(),
       CURRENT_WAREHOUSE()
"""


def check_connection(
    settings: SnowflakeSettings, connector: Any | None = None
) -> tuple[str, str, str, str, str]:
    """Connect to the configured development target and verify active context."""
    # Recheck fixed targets even when settings are constructed directly by callers.
    if (settings.database, settings.warehouse, settings.schema) != (
        "SUPPLY_CHAIN_DEV",
        "SUPPLY_CHAIN_DEV_WH",
        "RAW",
    ):
        raise ConfigurationError(
            "Snowflake connection target must be SUPPLY_CHAIN_DEV / RAW."
        )

    if connector is None:
        import snowflake.connector

        connector = snowflake.connector

    connection = connector.connect(**settings.connector_parameters())
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(CONTEXT_QUERY, timeout=15)
            row = cursor.fetchone()
        finally:
            cursor.close()
    finally:
        connection.close()

    if row is None or len(row) != 5:
        raise RuntimeError("Snowflake returned an invalid session context.")
    context = tuple(value.upper() if isinstance(value, str) else value for value in row)
    expected = (
        settings.user.upper(),
        settings.role.upper(),
        "SUPPLY_CHAIN_DEV",
        "RAW",
        "SUPPLY_CHAIN_DEV_WH",
    )
    if context != expected:
        raise RuntimeError(
            "Snowflake session context did not match the requested "
            "user, role, database, schema, and warehouse."
        )
    return tuple(row)  # type: ignore[return-value]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify read-only Snowflake connectivity to SUPPLY_CHAIN_DEV."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Environment file to load (default: .env).",
    )
    args = parser.parse_args(argv)
    load_dotenv(args.env_file, override=False)

    try:
        settings = SnowflakeSettings.from_environment()
        context = check_connection(settings)
    except ConfigurationError as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 2
    except Exception as exc:
        # Connector exceptions can contain credential or account details.
        LOGGER.error(
            "Snowflake development connectivity check failed (%s).",
            type(exc).__name__,
        )
        return 1

    print("Snowflake development connection verified.")
    print(f"User: {context[0]}")
    print(f"Role: {context[1]}")
    print(f"Database: {context[2]}")
    print(f"Schema: {context[3]}")
    print(f"Warehouse: {context[4]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
