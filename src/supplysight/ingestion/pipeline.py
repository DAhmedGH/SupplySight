"""Snowflake RAW loader using COPY staging and business-key MERGE."""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from supplysight.data_generation.schema import TABLE_COLUMNS
from supplysight.ingestion.contracts import (
    BUSINESS_KEYS,
    ENTITY_ORDER,
    SOURCE_SYSTEM,
)
from supplysight.ingestion.preflight import (
    FileManifest,
    PreflightResult,
    preflight_dataset,
)
from supplysight.settings import ConfigurationError, SnowflakeSettings

LOGGER = logging.getLogger("supplysight.ingestion")
CONTEXT_SQL = (
    "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), "
    "CURRENT_SCHEMA(), CURRENT_WAREHOUSE()"
)


@dataclass(frozen=True)
class IngestionResult:
    batch_id: str
    source_rows: int
    inserted_rows: int
    updated_rows: int
    unchanged_rows: int
    warning_rows: int
    quarantined_rows: int


def _guard_target(settings: SnowflakeSettings) -> None:
    expected = ("SUPPLY_CHAIN_DEV", "RAW", "SUPPLY_CHAIN_DEV_WH")
    if (
        settings.database.upper(),
        settings.schema.upper(),
        settings.warehouse.upper(),
    ) != expected:
        raise ConfigurationError(
            "Ingestion target must be SUPPLY_CHAIN_DEV.RAW using SUPPLY_CHAIN_DEV_WH."
        )
    if any(
        blocked in settings.role.upper()
        for blocked in ("PROD", "ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN")
    ):
        raise ConfigurationError("Ingestion role is not permitted for a DEV load.")


def _connect(settings: SnowflakeSettings, connector: Any | None) -> Any:
    _guard_target(settings)
    if connector is None:
        import snowflake.connector

        connector = snowflake.connector
    return connector.connect(**settings.connector_parameters())


def _execute(cursor: Any, sql: str, params: tuple[Any, ...] | None = None) -> Any:
    return cursor.execute(sql, params) if params is not None else cursor.execute(sql)


def _verify_session(cursor: Any, settings: SnowflakeSettings) -> tuple[str, ...]:
    _execute(cursor, CONTEXT_SQL)
    row = cursor.fetchone()
    expected = (
        settings.user.upper(),
        settings.role.upper(),
        "SUPPLY_CHAIN_DEV",
        "RAW",
        "SUPPLY_CHAIN_DEV_WH",
    )
    actual = tuple(
        item.upper() if isinstance(item, str) else item for item in (row or ())
    )
    if actual != expected:
        raise ConfigurationError(
            "Active Snowflake session does not match the configured DEV user, role, "
            "database, schema, and warehouse."
        )
    return tuple(row)


def _ensure_raw_objects(cursor: Any, ddl_path: Path) -> None:
    """Apply the approved fixed-target DDL and reject incompatible existing tables."""
    ddl = ddl_path.read_text(encoding="utf-8")
    ddl = re.sub(r"--[^\r\n]*", "", ddl)
    statements = re.findall(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+SUPPLY_CHAIN_DEV\.RAW\.[A-Z_]+\s*\(.*?\)\s*;",
        ddl,
        flags=re.I | re.S,
    )
    if len(statements) != 12:
        raise RuntimeError(
            f"Expected 12 approved RAW DDL statements, found {len(statements)}."
        )
    expected: dict[str, dict[str, str]] = {}
    for statement in statements:
        match = re.search(r"SUPPLY_CHAIN_DEV\.RAW\.([A-Z_]+)", statement, flags=re.I)
        if not match:
            raise RuntimeError("RAW DDL contains an invalid object name.")
        name = match.group(1).upper()
        expected[name] = {
            column.upper(): data_type.upper()
            for column, data_type in re.findall(
                r"^\s*([A-Z_][A-Z_0-9]*)\s+"
                r"(VARCHAR|TIMESTAMP_TZ|ARRAY|NUMBER)(?:\([^)]*\))?",
                statement,
                flags=re.I | re.M,
            )
        }
        _execute(cursor, statement)
    for name, expected_columns in expected.items():
        _execute(cursor, f"DESC TABLE SUPPLY_CHAIN_DEV.RAW.{name}")
        rows = cursor.fetchall()
        actual_columns = {str(row[0]).upper(): str(row[1]).upper() for row in rows}
        missing = sorted(set(expected_columns) - set(actual_columns))
        mismatched = sorted(
            column
            for column, expected_type in expected_columns.items()
            if column in actual_columns
            and not actual_columns[column].startswith(expected_type)
        )
        if missing or mismatched:
            raise RuntimeError(
                f"Existing RAW table {name} has incompatible columns; "
                f"missing={missing}, type_mismatches={mismatched}."
            )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _stage_table(entity: str, batch_id: str) -> str:
    return f"TMP_{entity.upper()}_{batch_id.replace('-', '')[:12].upper()}"


def _create_stage(cursor: Any, entity: str, temp: str) -> None:
    columns = ", ".join(f'"{column}" VARCHAR' for column in TABLE_COLUMNS[entity])
    columns += (
        ', "_ROW_HASH" VARCHAR, "_QUALITY_STATUS" VARCHAR, "_QUALITY_ISSUES" ARRAY'
    )
    _execute(cursor, f'CREATE TEMPORARY TABLE "{temp}" ({columns})')


def _put_copy(cursor: Any, path: Path, batch_id: str, temp: str) -> None:
    stage_path = f"@~/SUPPLYSIGHT/{batch_id}"
    local_uri = "file://" + path.resolve().as_posix()
    _execute(
        cursor,
        f"PUT {_literal(local_uri)} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
    )
    source_column_sql = ", ".join(
        '"' + column + '"' for column in TABLE_COLUMNS[path.stem]
    )
    _execute(
        cursor,
        f'COPY INTO "{temp}" ({source_column_sql}) '
        f"FROM {stage_path}/{path.name} FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 "
        "FIELD_OPTIONALLY_ENCLOSED_BY='\"' EMPTY_FIELD_AS_NULL=FALSE) "
        "ON_ERROR=ABORT_STATEMENT",
    )


def _assert_stage_count(cursor: Any, temp: str, expected: int) -> None:
    _execute(cursor, f'SELECT COUNT(*) FROM "{temp}"')
    actual = int(cursor.fetchone()[0])
    if actual != expected:
        raise RuntimeError(
            f"Snowflake staged {actual} rows; local preflight counted {expected}."
        )


def _verify_file_manifest(manifest: FileManifest) -> None:
    """Reject source changes since preflight, immediately before staging upload."""
    digest = hashlib.sha256()
    size = 0
    with manifest.path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    if size != manifest.size_bytes or digest.hexdigest() != manifest.sha256:
        raise RuntimeError(
            f"Source file changed after preflight: {manifest.path.name}."
        )


def _decorate_stage(cursor: Any, entity: str, temp: str) -> None:
    cols = TABLE_COLUMNS[entity]
    # Hash exact source strings, including blank optional values, for change detection.
    hash_expr = (
        "SHA2(TO_JSON(ARRAY_CONSTRUCT(" + ", ".join(f'"{c}"' for c in cols) + ")), 256)"
    )
    predicates = {
        "products": [("\"subcategory\" = ''", "missing_optional_attribute")],
        "shipments": [
            (
                "\"shipment_status\" = 'delivered' AND \"delivery_date\" = ''",
                "missing_delivery_timestamp",
            )
        ],
        "customers": [('"region" != INITCAP("region")', "nonstandard_region_case")],
        "orders": [
            (
                'TRY_TO_DECIMAL("discount_pct", 38, 18) < 0 OR '
                'TRY_TO_DECIMAL("discount_pct", 38, 18) > 1',
                "out_of_range_discount",
            )
        ],
        "inventory_snapshots": [
            (
                'TRY_TO_NUMBER("available_units") != '
                'TRY_TO_NUMBER("on_hand_units") - '
                'TRY_TO_NUMBER("reserved_units")',
                "inventory_balance_mismatch",
            )
        ],
    }
    issue_exprs = [
        f"IFF({predicate}, {_literal(issue)}, NULL)"
        for predicate, issue in predicates.get(entity, [])
    ]
    issues = "ARRAY_CONSTRUCT_COMPACT(" + ", ".join(issue_exprs) + ")"
    quarantine = entity in {"shipments", "orders", "inventory_snapshots"}
    quality_label = "QUARANTINED" if quarantine else "WARNING"
    status = f"IFF(ARRAY_SIZE({issues})=0,'CLEAN','{quality_label}')"
    _execute(
        cursor,
        f'UPDATE "{temp}" SET "_ROW_HASH"={hash_expr}, '
        f'"_QUALITY_ISSUES"={issues}, "_QUALITY_STATUS"={status}',
    )


def _merge_file(
    cursor: Any, entity: str, temp: str, source_file: str, batch_id: str
) -> tuple[int, int, int, int, int, int]:
    target = f"SUPPLY_CHAIN_DEV.RAW.{entity.upper()}"
    cols = TABLE_COLUMNS[entity]
    keys = BUSINESS_KEYS[entity]
    on = " AND ".join(f't."{k.upper()}"=s."{k}"' for k in keys)
    quality_equal = (
        't."_QUALITY_STATUS"=s."_QUALITY_STATUS" AND '
        "COALESCE(TO_JSON(t.\"_QUALITY_ISSUES\"), 'null')="
        "COALESCE(TO_JSON(s.\"_QUALITY_ISSUES\"), 'null')"
    )
    changed = (
        't."_ROW_HASH"<>s."_ROW_HASH" OR '
        't."_QUALITY_STATUS"<>s."_QUALITY_STATUS" OR '
        "COALESCE(TO_JSON(t.\"_QUALITY_ISSUES\"), 'null')<>"
        "COALESCE(TO_JSON(s.\"_QUALITY_ISSUES\"), 'null')"
    )
    _execute(
        cursor,
        f"SELECT COUNT(*), COUNT_IF(\"_QUALITY_STATUS\"='WARNING'), "
        f'COUNT_IF("_QUALITY_STATUS"=\'QUARANTINED\') FROM "{temp}"',
    )
    source_count, warning_count, quarantined_count = cursor.fetchone()
    _execute(
        cursor,
        f'SELECT COUNT(*) FROM "{temp}" s JOIN {target} t ON {on} '
        f'WHERE t."_ROW_HASH"=s."_ROW_HASH" AND {quality_equal}',
    )
    unchanged = cursor.fetchone()[0]
    metadata = [
        "_INGESTED_AT",
        "_SOURCE_FILE",
        "_BATCH_ID",
        "_SOURCE_SYSTEM",
        "_ROW_HASH",
        "_QUALITY_STATUS",
        "_QUALITY_ISSUES",
    ]
    update_cols = [c for c in cols if c not in keys] + metadata
    update_set = ", ".join(
        f't."{c.upper()}"=s."{c}"'
        for c in update_cols
        if c not in {"_INGESTED_AT", "_SOURCE_FILE", "_BATCH_ID", "_SOURCE_SYSTEM"}
    )
    update_set += (
        ', t."_INGESTED_AT"=CURRENT_TIMESTAMP(), '
        f't."_SOURCE_FILE"={_literal(source_file)}, '
        f't."_BATCH_ID"={_literal(batch_id)}, '
        f't."_SOURCE_SYSTEM"={_literal(SOURCE_SYSTEM)}'
    )
    insert_cols = [*cols, *metadata]
    insert_values = [f's."{c}"' for c in cols] + [
        "CURRENT_TIMESTAMP()",
        _literal(source_file),
        _literal(batch_id),
        _literal(SOURCE_SYSTEM),
        's."_ROW_HASH"',
        's."_QUALITY_STATUS"',
        's."_QUALITY_ISSUES"',
    ]
    insert_column_sql = ", ".join('"' + c.upper() + '"' for c in insert_cols)
    merge = (
        f'MERGE INTO {target} t USING "{temp}" s ON {on} '
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_column_sql}) "
        f"VALUES ({', '.join(insert_values)})"
    )
    _execute(cursor, merge)
    result = cursor.fetchone()
    # Snowflake MERGE returns inserted, updated, deleted counts in its result row.
    inserted = updated = 0
    if result and len(result) >= 2:
        inserted, updated = int(result[0]), int(result[1])
    if inserted + updated + int(unchanged) != int(source_count):
        raise RuntimeError(
            f"MERGE counts do not reconcile for {entity}: staged={source_count}, "
            f"inserted={inserted}, updated={updated}, unchanged={unchanged}."
        )
    quarantine_key_sql = ", ".join('s."' + key + '"' for key in keys)
    payload_sql = ", ".join(_literal(column) + ',s."' + column + '"' for column in cols)
    quarantine_sql = (
        "INSERT INTO SUPPLY_CHAIN_DEV.RAW.INGESTION_QUARANTINE "
        "(QUARANTINE_ID,BATCH_ID,ENTITY_NAME,BUSINESS_KEY,SOURCE_FILE,SOURCE_SYSTEM,"
        "ROW_HASH,QUALITY_STATUS,QUALITY_ISSUES,SOURCE_PAYLOAD,QUARANTINED_AT) "
        f"SELECT UUID_STRING(),{_literal(batch_id)},{_literal(entity)},"
        f"TO_VARCHAR(ARRAY_CONSTRUCT({quarantine_key_sql})),"
        f'{_literal(source_file)},{_literal(SOURCE_SYSTEM)},s."_ROW_HASH",'
        's."_QUALITY_STATUS",s."_QUALITY_ISSUES",'
        f"OBJECT_CONSTRUCT_KEEP_NULL({payload_sql}),CURRENT_TIMESTAMP() "
        f'FROM "{temp}" s WHERE s."_QUALITY_STATUS"=\'QUARANTINED\''
    )
    _execute(cursor, quarantine_sql)
    return (
        int(source_count),
        int(warning_count),
        int(quarantined_count),
        unchanged,
        inserted,
        updated,
    )


def ingest_dataset(
    input_dir: Path | str,
    settings: SnowflakeSettings,
    *,
    connector: Any | None = None,
    preflight: PreflightResult | None = None,
) -> IngestionResult:
    """Preflight all nine files, then merge them to the fixed DEV RAW schema."""
    _guard_target(settings)
    validation = preflight or preflight_dataset(input_dir)
    batch_id = str(uuid.uuid4())
    connection = _connect(settings, connector)
    if hasattr(connection, "autocommit"):
        connection.autocommit(False)
    cursor = connection.cursor()
    totals = {
        "source": 0,
        "warning": 0,
        "quarantine": 0,
        "unchanged": 0,
        "inserted": 0,
        "updated": 0,
    }
    active_entity: str | None = None
    try:
        _verify_session(cursor, settings)
        ddl_path = (
            Path(__file__).resolve().parents[3] / "sql" / "raw" / "001_raw_tables.sql"
        )
        _ensure_raw_objects(cursor, ddl_path)

        # Snowflake DDL can commit an active transaction. Create and populate all
        # session-local stages before opening the all-or-nothing RAW transaction.
        staged: dict[str, tuple[str, Any]] = {}
        for entity in ENTITY_ORDER:
            active_entity = entity
            manifest = validation.files[entity]
            temp = _stage_table(entity, batch_id)
            _create_stage(cursor, entity, temp)
            _verify_file_manifest(manifest)
            _put_copy(cursor, manifest.path, batch_id, temp)
            _assert_stage_count(cursor, temp, manifest.row_count)
            _decorate_stage(cursor, entity, temp)
            staged[entity] = (temp, manifest)

        # Finish any transaction opened implicitly by COPY before business DML.
        connection.commit()
        _execute(cursor, "BEGIN")
        _execute(
            cursor,
            "INSERT INTO SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCHES "
            "(BATCH_ID,SOURCE_SYSTEM,STATUS,STARTED_AT,"
            "SOURCE_FILE_COUNT,SOURCE_ROW_COUNT) "
            "VALUES (%s,%s,'RUNNING',CURRENT_TIMESTAMP(),%s,%s)",
            (
                batch_id,
                SOURCE_SYSTEM,
                len(ENTITY_ORDER),
                sum(validation.row_counts.values()),
            ),
        )
        for entity in ENTITY_ORDER:
            active_entity = entity
            temp, manifest = staged[entity]
            file_name = manifest.path.name
            _execute(
                cursor,
                "INSERT INTO SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCH_FILES "
                "(BATCH_ID,SOURCE_FILE,ENTITY_NAME,FILE_SHA256,FILE_SIZE_BYTES,"
                "STATUS,SOURCE_ROW_COUNT,STARTED_AT) "
                "VALUES (%s,%s,%s,%s,%s,'RUNNING',%s,CURRENT_TIMESTAMP())",
                (
                    batch_id,
                    file_name,
                    entity.upper(),
                    manifest.sha256,
                    manifest.size_bytes,
                    manifest.row_count,
                ),
            )
            source, warning, quarantine, unchanged, inserted, updated = _merge_file(
                cursor, entity, temp, file_name, batch_id
            )
            for key, value in (
                ("source", source),
                ("warning", warning),
                ("quarantine", quarantine),
                ("unchanged", unchanged),
                ("inserted", inserted),
                ("updated", updated),
            ):
                totals[key] += value
            _execute(
                cursor,
                "UPDATE SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCH_FILES SET "
                "STATUS='SUCCEEDED',MERGED_ROW_COUNT=%s,INSERTED_ROW_COUNT=%s,"
                "UPDATED_ROW_COUNT=%s,UNCHANGED_ROW_COUNT=%s,WARNING_ROW_COUNT=%s,"
                "QUARANTINED_ROW_COUNT=%s,QUARANTINE_RECORD_COUNT=%s,"
                "HARD_REJECTED_ROW_COUNT=0,COMPLETED_AT=CURRENT_TIMESTAMP() "
                "WHERE BATCH_ID=%s AND SOURCE_FILE=%s",
                (
                    source,
                    inserted,
                    updated,
                    unchanged,
                    warning,
                    quarantine,
                    quarantine,
                    batch_id,
                    file_name,
                ),
            )
        _execute(
            cursor,
            "UPDATE SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCHES SET "
            "STATUS='SUCCEEDED',COMPLETED_AT=CURRENT_TIMESTAMP(),"
            "MERGED_ROW_COUNT=%s,INSERTED_ROW_COUNT=%s,UPDATED_ROW_COUNT=%s,"
            "UNCHANGED_ROW_COUNT=%s,WARNING_ROW_COUNT=%s,QUARANTINED_ROW_COUNT=%s,"
            "QUARANTINE_RECORD_COUNT=%s,HARD_REJECTED_ROW_COUNT=0 WHERE BATCH_ID=%s",
            (
                totals["source"],
                totals["inserted"],
                totals["updated"],
                totals["unchanged"],
                totals["warning"],
                totals["quarantine"],
                totals["quarantine"],
                batch_id,
            ),
        )
        connection.commit()
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            LOGGER.exception("Rollback failed for ingestion batch %s", batch_id)
        LOGGER.exception("Ingestion batch %s failed", batch_id)
        try:
            _execute(cursor, "BEGIN")
            cursor.execute(
                "MERGE INTO SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCHES t "
                "USING (SELECT %s BATCH_ID,%s SOURCE_SYSTEM,%s ERROR_MESSAGE) s "
                "ON t.BATCH_ID=s.BATCH_ID "
                "WHEN MATCHED THEN UPDATE SET STATUS='FAILED',"
                "COMPLETED_AT=CURRENT_TIMESTAMP(),ERROR_MESSAGE=s.ERROR_MESSAGE "
                "WHEN NOT MATCHED THEN INSERT "
                "(BATCH_ID,SOURCE_SYSTEM,STATUS,STARTED_AT,COMPLETED_AT,ERROR_MESSAGE) "
                "VALUES (s.BATCH_ID,s.SOURCE_SYSTEM,'FAILED',CURRENT_TIMESTAMP(),"
                "CURRENT_TIMESTAMP(),s.ERROR_MESSAGE)",
                (batch_id, SOURCE_SYSTEM, type(exc).__name__),
            )
            if active_entity is not None:
                manifest = validation.files[active_entity]
                cursor.execute(
                    "MERGE INTO SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCH_FILES t "
                    "USING (SELECT %s BATCH_ID,%s SOURCE_FILE,%s ENTITY_NAME,"
                    "%s FILE_SHA256,%s FILE_SIZE_BYTES,%s SOURCE_ROW_COUNT,"
                    "%s ERROR_MESSAGE) s "
                    "ON t.BATCH_ID=s.BATCH_ID AND t.SOURCE_FILE=s.SOURCE_FILE "
                    "WHEN MATCHED THEN UPDATE SET STATUS='FAILED',"
                    "COMPLETED_AT=CURRENT_TIMESTAMP(),ERROR_MESSAGE=s.ERROR_MESSAGE "
                    "WHEN NOT MATCHED THEN INSERT "
                    "(BATCH_ID,SOURCE_FILE,ENTITY_NAME,FILE_SHA256,FILE_SIZE_BYTES,"
                    "SOURCE_ROW_COUNT,STATUS,STARTED_AT,COMPLETED_AT,ERROR_MESSAGE) "
                    "VALUES (s.BATCH_ID,s.SOURCE_FILE,s.ENTITY_NAME,s.FILE_SHA256,"
                    "s.FILE_SIZE_BYTES,s.SOURCE_ROW_COUNT,'FAILED',CURRENT_TIMESTAMP(),"
                    "CURRENT_TIMESTAMP(),s.ERROR_MESSAGE)",
                    (
                        batch_id,
                        manifest.path.name,
                        active_entity.upper(),
                        manifest.sha256,
                        manifest.size_bytes,
                        manifest.row_count,
                        type(exc).__name__,
                    ),
                )
            connection.commit()
        except Exception:
            LOGGER.exception("Could not persist FAILED batch status %s", batch_id)
        raise
    finally:
        cursor.close()
        connection.close()
    return IngestionResult(
        batch_id,
        totals["source"],
        totals["inserted"],
        totals["updated"],
        totals["unchanged"],
        totals["warning"],
        totals["quarantine"],
    )
