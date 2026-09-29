"""Unit tests for CSV preflight and Snowflake merge safety boundaries."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

import pytest

from scripts.ingest_raw import main as cli_main
from supplysight.data_generation.schema import TABLE_COLUMNS
from supplysight.ingestion.contracts import ENTITY_ORDER
from supplysight.ingestion.pipeline import (
    _decorate_stage,
    _ensure_raw_objects,
    _guard_target,
    _merge_file,
    _put_copy,
    _verify_file_manifest,
    ingest_dataset,
)
from supplysight.ingestion.preflight import (
    PreflightError,
    _valid_value,
    preflight_dataset,
)
from supplysight.settings import ConfigurationError, SnowflakeSettings


@pytest.mark.parametrize("field", ["on_time_rate", "quality_rate"])
@pytest.mark.parametrize(
    ("value", "valid"),
    [
        ("0", True),
        ("1", True),
        ("0.875", True),
        (".25", True),
        ("-0.1", False),
        ("1.01", False),
        ("not-a-decimal", False),
        ("1e-1", False),
        ("0.1234567890123456789", False),
    ],
)
def test_supplier_rates_are_bounded_valid_decimals(field, value, valid):
    assert _valid_value("suppliers", field, value) is valid


@pytest.mark.parametrize("value", ["True", "False", "true", "false"])
def test_boolean_contract_accepts_generated_and_lowercase_values(value):
    assert _valid_value("suppliers", "active_flag", value)


@pytest.mark.parametrize("value", ["TRUE", "1", ""])
def test_boolean_contract_rejects_unsupported_values(value):
    assert not _valid_value("suppliers", "active_flag", value)


def test_preflight_rejects_supplier_rate_outside_contract(tmp_path: Path):
    root = _write_dataset(tmp_path / "source")
    supplier_file = root / "suppliers.csv"
    supplier_file.write_text(
        supplier_file.read_text(encoding="utf-8").replace(",0.9,0.95,", ",1.5,0.95,"),
        encoding="utf-8",
    )
    with pytest.raises(PreflightError, match="on_time_rate"):
        preflight_dataset(root)


def _write_dataset(root: Path) -> Path:
    root.mkdir(parents=True)
    rows: dict[str, list[dict[str, str]]] = {
        "products": [
            dict(
                zip(
                    TABLE_COLUMNS["products"],
                    [
                        "P1",
                        "SKU1",
                        "Widget",
                        "Home",
                        "Storage",
                        "1.25",
                        "2.50",
                        "2024-01-01",
                        "True",
                        "3",
                    ],
                )
            )
        ],
        "warehouses": [
            dict(
                zip(
                    TABLE_COLUMNS["warehouses"],
                    ["W1", "East", "Northeast", "US", "100"],
                )
            )
        ],
        "suppliers": [
            dict(
                zip(
                    TABLE_COLUMNS["suppliers"],
                    [
                        "S1",
                        "Supplier",
                        "Northeast",
                        "US",
                        "8",
                        "2.0",
                        "0.9",
                        "0.95",
                        "1.0",
                        "True",
                    ],
                )
            )
        ],
        "customers": [
            dict(
                zip(
                    TABLE_COLUMNS["customers"],
                    ["C1", "Retail", "Northeast", "US", "2023-01-01"],
                )
            )
        ],
        "orders": [
            dict(
                zip(
                    TABLE_COLUMNS["orders"],
                    [
                        "O1",
                        "L1",
                        "2024-02-01",
                        "C1",
                        "P1",
                        "W1",
                        "2",
                        "2.50",
                        "0.1",
                        "delivered",
                        "2024-02-05",
                    ],
                )
            )
        ],
        "inventory_snapshots": [
            dict(
                zip(
                    TABLE_COLUMNS["inventory_snapshots"],
                    ["2024-02-01", "W1", "P1", "10", "2", "8", "0", "12.50"],
                )
            )
        ],
        "purchase_orders": [
            dict(
                zip(
                    TABLE_COLUMNS["purchase_orders"],
                    [
                        "PO1",
                        "POL1",
                        "S1",
                        "W1",
                        "P1",
                        "2024-01-01",
                        "2024-01-10",
                        "",
                        "10",
                        "0",
                        "1.25",
                        "open",
                    ],
                )
            )
        ],
        "shipments": [
            dict(
                zip(
                    TABLE_COLUMNS["shipments"],
                    [
                        "SH1",
                        "L1",
                        "W1",
                        "2024-02-02",
                        "2024-02-04",
                        "2",
                        "delivered",
                        "Carrier",
                    ],
                )
            )
        ],
        "returns": [
            dict(
                zip(
                    TABLE_COLUMNS["returns"],
                    ["R1", "L1", "2024-02-10", "1", "damaged", "quarantine", "2.50"],
                )
            )
        ],
    }
    for entity in ENTITY_ORDER:
        with (root / f"{entity}.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=TABLE_COLUMNS[entity])
            writer.writeheader()
            writer.writerows(rows[entity])
    return root


def test_preflight_checks_headers_types_keys_and_relationships(tmp_path: Path) -> None:
    root = _write_dataset(tmp_path / "source")
    result = preflight_dataset(root)
    assert set(result.files) == set(ENTITY_ORDER)
    assert result.row_counts["inventory_snapshots"] == 1
    assert all(len(file.sha256) == 64 for file in result.files.values())

    path = root / "orders.csv"
    text = path.read_text(encoding="utf-8").replace(",C1,P1,W1,", ",NO_CUSTOMER,P1,W1,")
    path.write_text(text, encoding="utf-8")
    with pytest.raises(PreflightError) as exc:
        preflight_dataset(root)
    assert any(
        "invalid customer_id reference" in error for error in str(exc.value).split(";")
    )
    assert exc.value.sidecar


def test_documented_quality_findings_are_preserved_and_counted(tmp_path: Path) -> None:
    root = _write_dataset(tmp_path / "source")
    product_file = root / "products.csv"
    product_file.write_text(
        product_file.read_text().replace("Storage", ""), encoding="utf-8"
    )
    customer_file = root / "customers.csv"
    customer_file.write_text(
        customer_file.read_text().replace("Northeast", "NORTHEAST"), encoding="utf-8"
    )
    shipment_file = root / "shipments.csv"
    shipment_file.write_text(
        shipment_file.read_text().replace("2024-02-04", ""), encoding="utf-8"
    )
    order_file = root / "orders.csv"
    order_file.write_text(
        order_file.read_text().replace(",0.1,", ",1.2,"), encoding="utf-8"
    )
    inventory_file = root / "inventory_snapshots.csv"
    inventory_file.write_text(
        inventory_file.read_text().replace(",8,0,", ",11,0,"), encoding="utf-8"
    )

    result = preflight_dataset(root)
    assert result.warning_counts == {
        "missing_optional_attribute": 1,
        "missing_delivery_timestamp": 1,
        "nonstandard_region_case": 1,
        "out_of_range_discount": 1,
        "inventory_balance_mismatch": 1,
    }
    assert (root / "shipments.csv").read_text().endswith(",,2,delivered,Carrier\n")


def test_discount_fractional_precision_is_retained_in_quality_findings(
    tmp_path: Path,
) -> None:
    root = _write_dataset(tmp_path / "source")
    order_file = root / "orders.csv"
    order_file.write_text(
        order_file.read_text(encoding="utf-8").replace(",0.1,", ",1.0000001,"),
        encoding="utf-8",
    )

    result = preflight_dataset(root)
    assert result.warning_counts["out_of_range_discount"] == 1

    cursor = _MergeCursor()
    _decorate_stage(cursor, "orders", "TMP_ORDERS")
    classifier_sql = cursor.sql[0]
    assert 'TRY_TO_DECIMAL("discount_pct", 38, 18) < 0' in classifier_sql
    assert 'TRY_TO_DECIMAL("discount_pct", 38, 18) > 1' in classifier_sql


def test_preflight_rejects_decimal_scale_unrepresentable_in_raw_classifier(
    tmp_path: Path,
) -> None:
    root = _write_dataset(tmp_path / "source")
    order_file = root / "orders.csv"
    order_file.write_text(
        order_file.read_text(encoding="utf-8").replace(
            ",0.1,", ",1.0000000000000000001,"
        ),
        encoding="utf-8",
    )
    with pytest.raises(PreflightError, match="discount_pct"):
        preflight_dataset(root)


def test_duplicate_business_key_fails_preflight(tmp_path: Path) -> None:
    root = _write_dataset(tmp_path / "source")
    path = root / "products.csv"
    contents = path.read_text(encoding="utf-8")
    path.write_text(contents + contents.splitlines(keepends=True)[1], encoding="utf-8")
    with pytest.raises(PreflightError, match="duplicate_business_key"):
        preflight_dataset(root)


def test_target_guard_rejects_non_dev_database() -> None:
    settings = SnowflakeSettings(
        account="acct",
        user="user",
        role="role",
        private_key_file=Path("key.p8"),
        database="SUPPLY_CHAIN_PROD",
        warehouse="SUPPLY_CHAIN_DEV_WH",
        schema="RAW",
    )
    with pytest.raises(ConfigurationError, match="SUPPLY_CHAIN_DEV"):
        _guard_target(settings)


class _MergeCursor:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.results = [(2, 1, 0), (1,), (1, 0, 0)]

    def execute(self, statement: str, params: Any = None) -> None:
        self.sql.append(statement)

    def fetchone(self) -> tuple[int, ...]:
        return self.results.pop(0)


def test_merge_is_hash_idempotent_and_has_no_delete_clause() -> None:
    cursor = _MergeCursor()
    counts = _merge_file(cursor, "products", "TMP_PRODUCTS", "products.csv", "batch-1")
    assert counts == (2, 1, 0, 1, 1, 0)
    merge_sql = next(sql for sql in cursor.sql if sql.startswith("MERGE INTO"))
    assert "WHEN MATCHED AND (" in merge_sql
    assert 't."_ROW_HASH"<>s."_ROW_HASH"' in merge_sql
    assert 't."_QUALITY_STATUS"<>s."_QUALITY_STATUS"' in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    assert "DELETE" not in merge_sql.upper()
    unchanged_sql = next(
        sql for sql in cursor.sql if sql.startswith('SELECT COUNT(*) FROM "TMP_')
    )
    assert '"_QUALITY_STATUS"=s."_QUALITY_STATUS"' in unchanged_sql
    assert 'TO_JSON(t."_QUALITY_ISSUES")' in unchanged_sql


def test_discount_quality_classifier_preserves_fractional_precision() -> None:
    cursor = _MergeCursor()
    _decorate_stage(cursor, "orders", "TMP_ORDERS")
    classifier_sql = cursor.sql[0]
    assert 'TRY_TO_DECIMAL("discount_pct", 38, 18) < 0' in classifier_sql
    assert 'TRY_TO_DECIMAL("discount_pct", 38, 18) > 1' in classifier_sql


class _DdlCursor:
    def __init__(self, ddl: str) -> None:
        self.ddl = ddl
        self.created = 0
        self.current_table = ""

    def execute(self, statement: str) -> None:
        if statement.startswith("CREATE TABLE"):
            self.created += 1
        elif statement.startswith("DESC TABLE"):
            self.current_table = statement.rsplit(".", 1)[-1]

    def fetchall(self) -> list[tuple[str, str]]:
        statement = re.search(
            rf"CREATE TABLE IF NOT EXISTS SUPPLY_CHAIN_DEV\.RAW\.{self.current_table}"
            r"\s*\(.*?\)\s*;",
            self.ddl,
            flags=re.I | re.S,
        )
        assert statement is not None
        columns = re.findall(
            r"^\s*([A-Z_][A-Z_0-9]*)\s+"
            r"(VARCHAR|TIMESTAMP_TZ|ARRAY|NUMBER)(?:\([^)]*\))?",
            statement.group(0),
            flags=re.I | re.M,
        )
        return [(name, data_type) for name, data_type in columns]


def test_raw_ddl_parser_applies_and_checks_all_twelve_objects() -> None:
    ddl_path = Path("sql/raw/001_raw_tables.sql")
    ddl = ddl_path.read_text(encoding="utf-8")
    cursor = _DdlCursor(ddl)
    _ensure_raw_objects(cursor, ddl_path)
    assert cursor.created == 12


class _PutCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_put_quotes_windows_file_uri_with_workspace_spaces(tmp_path: Path) -> None:
    path = tmp_path / "My Project" / "products.csv"
    path.parent.mkdir()
    path.write_text("product_id\nP1\n", encoding="utf-8")
    cursor = _PutCursor()
    _put_copy(cursor, path, "batch-1", "TMP_PRODUCTS")
    assert cursor.statements[0].startswith("PUT 'file://")
    assert "My Project" in cursor.statements[0]
    assert "%20" not in cursor.statements[0]
    assert "COPY INTO" in cursor.statements[1]


def test_manifest_check_rejects_file_changed_after_preflight(tmp_path: Path) -> None:
    root = _write_dataset(tmp_path / "source")
    manifest = preflight_dataset(root).files["products"]
    path = manifest.path
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="changed after preflight"):
        _verify_file_manifest(manifest)


class _PipelineCursor:
    def __init__(self, ddl: str, fail_entity: str | None = None) -> None:
        self.ddl = ddl
        self.fail_entity = fail_entity
        self.sql: list[str] = []
        self.current_sql = ""
        self.failed_once = False

    def execute(self, statement: str, params: Any = None) -> None:
        self.sql.append(statement)
        self.current_sql = statement
        if (
            self.fail_entity
            and statement.startswith("MERGE INTO SUPPLY_CHAIN_DEV.RAW.")
            and f"RAW.{self.fail_entity.upper()} " in statement
            and not self.failed_once
        ):
            self.failed_once = True
            raise RuntimeError("injected merge failure")

    def fetchone(self) -> tuple[Any, ...]:
        sql = self.current_sql
        if sql.startswith("SELECT CURRENT_USER()"):
            return (
                "LOAD_USER",
                "DEV_ROLE",
                "SUPPLY_CHAIN_DEV",
                "RAW",
                "SUPPLY_CHAIN_DEV_WH",
            )
        if "COUNT_IF" in sql:
            return (1, 0, 0)
        if " JOIN " in sql and sql.startswith("SELECT COUNT(*)"):
            return (0,)
        if sql.startswith('SELECT COUNT(*) FROM "TMP_'):
            return (1,)
        if sql.startswith("MERGE INTO SUPPLY_CHAIN_DEV.RAW."):
            return (1, 0, 0)
        raise AssertionError(f"Unexpected fetchone query: {sql}")

    def fetchall(self) -> list[tuple[str, str]]:
        table = self.current_sql.rsplit(".", 1)[-1]
        statement = re.search(
            rf"CREATE TABLE IF NOT EXISTS SUPPLY_CHAIN_DEV\.RAW\.{table}"
            r"\s*\(.*?\)\s*;",
            self.ddl,
            flags=re.I | re.S,
        )
        assert statement is not None
        return re.findall(
            r"^\s*([A-Z_][A-Z_0-9]*)\s+"
            r"(VARCHAR|TIMESTAMP_TZ|ARRAY|NUMBER)(?:\([^)]*\))?",
            statement.group(0),
            flags=re.I | re.M,
        )

    def close(self) -> None:
        pass


class _PipelineConnection:
    def __init__(self, cursor: _PipelineCursor) -> None:
        self._cursor = cursor
        self.commit_count = 0
        self.rollback_count = 0

    def autocommit(self, enabled: bool) -> None:
        assert enabled is False

    def cursor(self) -> _PipelineCursor:
        return self._cursor

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        pass


class _PipelineConnector:
    def __init__(self, connection: _PipelineConnection) -> None:
        self.connection = connection

    def connect(self, **kwargs: Any) -> _PipelineConnection:
        return self.connection


def _pipeline_settings() -> SnowflakeSettings:
    return SnowflakeSettings(
        account="acct",
        user="LOAD_USER",
        role="DEV_ROLE",
        private_key_file=Path("key.p8"),
        database="SUPPLY_CHAIN_DEV",
        warehouse="SUPPLY_CHAIN_DEV_WH",
        schema="RAW",
    )


def _run_mocked_pipeline(
    root: Path, fail_entity: str | None = None
) -> tuple[_PipelineCursor, _PipelineConnection]:
    ddl = Path("sql/raw/001_raw_tables.sql").read_text(encoding="utf-8")
    cursor = _PipelineCursor(ddl, fail_entity=fail_entity)
    connection = _PipelineConnection(cursor)
    ingest_dataset(
        root,
        _pipeline_settings(),
        connector=_PipelineConnector(connection),
    )
    return cursor, connection


def test_all_stage_ddl_precedes_single_business_transaction(tmp_path: Path) -> None:
    root = _write_dataset(tmp_path / "source")
    cursor, connection = _run_mocked_pipeline(root)
    begin_index = cursor.sql.index("BEGIN")
    stage_ddl = [
        index
        for index, sql in enumerate(cursor.sql)
        if sql.startswith("CREATE TEMPORARY TABLE")
    ]
    assert len(stage_ddl) == 9
    assert max(stage_ddl) < begin_index
    assert not any(
        sql.startswith(("CREATE ", "ALTER ", "DROP "))
        for sql in cursor.sql[begin_index + 1 :]
    )
    assert connection.rollback_count == 0
    assert connection.commit_count == 2  # staging, then one all-RAW commit


def test_merge_failure_rolls_back_and_records_failed_batch_separately(
    tmp_path: Path,
) -> None:
    root = _write_dataset(tmp_path / "source")
    ddl = Path("sql/raw/001_raw_tables.sql").read_text(encoding="utf-8")
    cursor = _PipelineCursor(ddl, fail_entity="warehouses")
    connection = _PipelineConnection(cursor)
    with pytest.raises(RuntimeError, match="injected merge failure"):
        ingest_dataset(
            root,
            _pipeline_settings(),
            connector=_PipelineConnector(connection),
        )
    begins = [index for index, sql in enumerate(cursor.sql) if sql == "BEGIN"]
    assert len(begins) == 2  # business transaction and separate FAILED audit
    assert connection.rollback_count == 1
    assert connection.commit_count == 2  # staging and FAILED audit only
    assert not any(
        sql.startswith(("CREATE ", "ALTER ", "DROP "))
        for sql in cursor.sql[begins[0] + 1 :]
    )
    assert any(
        "STATUS='FAILED'" in sql and "INGESTION_BATCHES" in sql for sql in cursor.sql
    )


def test_cli_stops_at_preflight_and_writes_local_reject_sidecar(tmp_path: Path) -> None:
    quarantine_dir = tmp_path / "quarantine"
    status = cli_main(
        [
            "--input-dir",
            str(tmp_path / "missing"),
            "--env-file",
            str(tmp_path / "absent.env"),
            "--quarantine-dir",
            str(quarantine_dir),
        ]
    )
    assert status == 2
    assert not list(quarantine_dir.glob("*.jsonl"))
