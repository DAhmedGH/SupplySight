from pathlib import Path

import pytest

from supplysight.settings import ConfigurationError, SnowflakeSettings
from supplysight.snowflake_check import check_connection


def settings() -> SnowflakeSettings:
    return SnowflakeSettings(
        account="example-account",
        user="service-user",
        role="SUPPLYSIGHT_DEV_ROLE",
        private_key_file=Path("dev_key.p8"),
        database="SUPPLY_CHAIN_DEV",
        warehouse="SUPPLY_CHAIN_DEV_WH",
        schema="RAW",
    )


class FakeCursor:
    def __init__(self, context):
        self.context = context
        self.executed = []
        self.closed = False

    def execute(self, query, timeout=None):
        self.executed.append(query)
        self.timeout = timeout

    def fetchone(self):
        return self.context

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class FakeConnector:
    def __init__(self, context):
        self.cursor_instance = FakeCursor(context)
        self.connection = FakeConnection(self.cursor_instance)
        self.parameters = None

    def connect(self, **parameters):
        self.parameters = parameters
        return self.connection


def test_checks_context_and_closes_read_only_connection() -> None:
    fake = FakeConnector(
        (
            "SERVICE-USER",
            "SUPPLYSIGHT_DEV_ROLE",
            "SUPPLY_CHAIN_DEV",
            "RAW",
            "SUPPLY_CHAIN_DEV_WH",
        )
    )

    result = check_connection(settings(), fake)

    assert result == fake.cursor_instance.context
    assert "CURRENT_DATABASE()" in fake.cursor_instance.executed[0]
    assert fake.connection.closed
    assert fake.cursor_instance.closed
    assert fake.parameters["database"] == "SUPPLY_CHAIN_DEV"
    assert fake.parameters["login_timeout"] == 15
    assert fake.parameters["network_timeout"] == 15
    assert fake.parameters["socket_timeout"] == 15
    assert callable(fake.parameters["backoff_policy"])
    assert next(fake.parameters["backoff_policy"]()) == 1
    assert fake.cursor_instance.timeout == 15


def test_fails_when_active_context_does_not_match_requested_role() -> None:
    fake = FakeConnector(
        (
            "SERVICE-USER",
            "OTHER_ROLE",
            "SUPPLY_CHAIN_DEV",
            "RAW",
            "SUPPLY_CHAIN_DEV_WH",
        )
    )

    with pytest.raises(RuntimeError, match="did not match"):
        check_connection(settings(), fake)

    assert fake.connection.closed


def test_refuses_unsafe_directly_constructed_settings_before_connecting() -> None:
    fake = FakeConnector(
        (
            "SERVICE-USER",
            "SUPPLYSIGHT_DEV_ROLE",
            "PROD",
            "RAW",
            "SUPPLY_CHAIN_DEV_WH",
        )
    )
    unsafe = SnowflakeSettings(
        account="example-account",
        user="service-user",
        role="SUPPLYSIGHT_DEV_ROLE",
        private_key_file=Path("dev_key.p8"),
        database="PROD",
        warehouse="SUPPLY_CHAIN_DEV_WH",
        schema="RAW",
    )

    with pytest.raises(ConfigurationError):
        check_connection(unsafe, fake)
    assert fake.parameters is None
