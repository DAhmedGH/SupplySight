"""Environment-backed settings for the SupplySight application."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEVELOPMENT_DATABASE = "SUPPLY_CHAIN_DEV"
DEVELOPMENT_WAREHOUSE = "SUPPLY_CHAIN_DEV_WH"
INITIAL_SCHEMA = "RAW"


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is missing or unsafe."""


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    role: str
    private_key_file: Path
    database: str
    warehouse: str
    schema: str
    private_key_passphrase: str | None = None

    @classmethod
    def from_environment(
        cls, environ: dict[str, str] | None = None
    ) -> "SnowflakeSettings":
        """Read and validate connection settings without opening a connection."""
        values = os.environ if environ is None else environ
        required = (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_PRIVATE_KEY_FILE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_SCHEMA",
        )
        missing = [name for name in required if not values.get(name, "").strip()]
        if missing:
            raise ConfigurationError(
                "Missing required environment variables: " + ", ".join(missing)
            )

        database = values["SNOWFLAKE_DATABASE"].strip()
        warehouse = values["SNOWFLAKE_WAREHOUSE"].strip()
        schema = values["SNOWFLAKE_SCHEMA"].strip()
        if database.upper() != DEVELOPMENT_DATABASE:
            raise ConfigurationError(
                f"SNOWFLAKE_DATABASE must be {DEVELOPMENT_DATABASE}."
            )
        if warehouse.upper() != DEVELOPMENT_WAREHOUSE:
            raise ConfigurationError(
                f"SNOWFLAKE_WAREHOUSE must be {DEVELOPMENT_WAREHOUSE}."
            )
        if schema.upper() != INITIAL_SCHEMA:
            raise ConfigurationError(f"SNOWFLAKE_SCHEMA must be {INITIAL_SCHEMA}.")

        return cls(
            account=values["SNOWFLAKE_ACCOUNT"].strip(),
            user=values["SNOWFLAKE_USER"].strip(),
            role=values["SNOWFLAKE_ROLE"].strip(),
            private_key_file=Path(
                values["SNOWFLAKE_PRIVATE_KEY_FILE"].strip()
            ).expanduser(),
            database=DEVELOPMENT_DATABASE,
            warehouse=DEVELOPMENT_WAREHOUSE,
            schema=INITIAL_SCHEMA,
            private_key_passphrase=values.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            or None,
        )

    def connector_parameters(self) -> dict[str, str | int]:
        """Return connector options after enforcing the development target."""
        from snowflake.connector.backoff_policies import linear_backoff

        parameters = {
            "account": self.account,
            "user": self.user,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": str(self.private_key_file),
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "login_timeout": 15,
            "network_timeout": 15,
            "socket_timeout": 15,
            # A short capped delay combined with the connector's login/network
            # deadlines bounds retry waiting without rapid repeated requests.
            "backoff_policy": linear_backoff(
                factor=1, base=1, cap=2, enable_jitter=False
            ),
        }
        if self.private_key_passphrase:
            parameters["private_key_file_pwd"] = self.private_key_passphrase
        return parameters
