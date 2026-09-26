from pathlib import Path

import pytest

from supplysight.settings import ConfigurationError, SnowflakeSettings


def valid_environment() -> dict[str, str]:
    return {
        "SNOWFLAKE_ACCOUNT": "example-account",
        "SNOWFLAKE_USER": "service-user",
        "SNOWFLAKE_ROLE": "SUPPLYSIGHT_DEV_ROLE",
        "SNOWFLAKE_PRIVATE_KEY_FILE": "keys/dev_key.p8",
        "SNOWFLAKE_DATABASE": "SUPPLY_CHAIN_DEV",
        "SNOWFLAKE_WAREHOUSE": "SUPPLY_CHAIN_DEV_WH",
        "SNOWFLAKE_SCHEMA": "RAW",
    }


def test_loads_required_settings_and_optional_passphrase() -> None:
    environ = valid_environment()
    environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"] = "sample-passphrase"

    settings = SnowflakeSettings.from_environment(environ)

    assert settings.private_key_file == Path("keys/dev_key.p8")
    assert settings.private_key_passphrase == "sample-passphrase"
    assert settings.database == "SUPPLY_CHAIN_DEV"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SNOWFLAKE_DATABASE", "PROD"),
        ("SNOWFLAKE_WAREHOUSE", "PROD_WH"),
        ("SNOWFLAKE_SCHEMA", "PUBLIC"),
    ],
)
def test_rejects_targets_outside_development(name: str, value: str) -> None:
    environ = valid_environment()
    environ[name] = value

    with pytest.raises(ConfigurationError):
        SnowflakeSettings.from_environment(environ)


def test_reports_missing_required_variables_without_values() -> None:
    with pytest.raises(ConfigurationError, match="SNOWFLAKE_ACCOUNT"):
        SnowflakeSettings.from_environment({})
