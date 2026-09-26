"""Read-only reconciliation of a development RAW load and its identical rerun."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

from supplysight.data_generation.schema import TABLE_COLUMNS
from supplysight.ingestion.contracts import BUSINESS_KEYS, SOURCE_SYSTEM
from supplysight.ingestion.pipeline import _connect, _verify_session
from supplysight.settings import SnowflakeSettings


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate SUPPLY_CHAIN_DEV.RAW loads.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--first-batch", required=True)
    parser.add_argument("--rerun-batch", required=True)
    parser.add_argument("--correction-batch", required=True)
    parser.add_argument("--stable-batch", required=True)
    args = parser.parse_args()

    report = json.loads(args.source_report.read_text(encoding="utf-8"))
    expected = report["row_counts"]
    defects = report["defect_counts"]
    expected_quality = {
        "products": (defects["missing_optional_attribute"], 0),
        "customers": (defects["nonstandard_region_case"], 0),
        "orders": (0, defects["out_of_range_discount"]),
        "inventory_snapshots": (0, defects["inventory_balance_mismatch"]),
        "shipments": (0, defects["missing_delivery_timestamp"]),
    }
    load_dotenv(args.env_file, override=False)
    settings = SnowflakeSettings.from_environment()
    connection = _connect(settings, None)
    cursor = connection.cursor()
    errors: list[str] = []
    try:
        _verify_session(cursor, settings)
        for entity in TABLE_COLUMNS:
            cursor.execute(
                "SELECT COUNT(*), "
                "COUNT_IF(_QUALITY_STATUS='WARNING'), "
                "COUNT_IF(_QUALITY_STATUS='QUARANTINED'), "
                "COUNT_IF(_INGESTED_AT IS NULL OR _SOURCE_FILE IS NULL OR "
                "_BATCH_ID IS NULL OR _ROW_HASH IS NULL OR "
                "_SOURCE_SYSTEM != %s) "
                f"FROM SUPPLY_CHAIN_DEV.RAW.{entity.upper()}",
                (SOURCE_SYSTEM,),
            )
            total, warning_rows, quarantined_rows, invalid_metadata = cursor.fetchone()
            print(
                f"{entity}: total={total} "
                f"warnings={warning_rows or 0} quarantined={quarantined_rows or 0}"
            )
            if total != expected[entity]:
                errors.append(f"{entity}: RAW count does not match source report")
            if (warning_rows or 0, quarantined_rows or 0) != expected_quality.get(
                entity, (0, 0)
            ):
                errors.append(f"{entity}: quality counts do not match source report")
            if invalid_metadata:
                errors.append(f"{entity}: missing or invalid ingestion metadata")
            keys = ", ".join(key.upper() for key in BUSINESS_KEYS[entity])
            cursor.execute(
                f"SELECT COUNT(*) FROM (SELECT {keys} "
                f"FROM SUPPLY_CHAIN_DEV.RAW.{entity.upper()} "
                f"GROUP BY {keys} HAVING COUNT(*) > 1)"
            )
            if cursor.fetchone()[0]:
                errors.append(f"{entity}: duplicate business key")

        batch_metrics = {}
        for batch_id in (
            args.first_batch,
            args.rerun_batch,
            args.correction_batch,
            args.stable_batch,
        ):
            cursor.execute(
                "SELECT STATUS,SOURCE_ROW_COUNT,MERGED_ROW_COUNT,"
                "INSERTED_ROW_COUNT,UPDATED_ROW_COUNT,UNCHANGED_ROW_COUNT,"
                "WARNING_ROW_COUNT,QUARANTINED_ROW_COUNT,"
                "QUARANTINE_RECORD_COUNT "
                "FROM SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCHES WHERE BATCH_ID=%s",
                (batch_id,),
            )
            row = cursor.fetchone()
            if row is None:
                errors.append(f"missing batch audit: {batch_id}")
                continue
            batch_metrics[batch_id] = row
            print(f"batch {batch_id}: {row}")
            cursor.execute(
                "SELECT COUNT(*) FROM SUPPLY_CHAIN_DEV.RAW.INGESTION_QUARANTINE "
                "WHERE BATCH_ID=%s",
                (batch_id,),
            )
            quarantine_count = cursor.fetchone()[0]
            if quarantine_count != row[8]:
                errors.append(f"batch {batch_id}: quarantine sidecar count mismatch")
            cursor.execute(
                "SELECT COUNT(*),SUM(SOURCE_ROW_COUNT),SUM(MERGED_ROW_COUNT),"
                "SUM(INSERTED_ROW_COUNT),SUM(UPDATED_ROW_COUNT),"
                "SUM(UNCHANGED_ROW_COUNT),SUM(WARNING_ROW_COUNT),"
                "SUM(QUARANTINED_ROW_COUNT),SUM(QUARANTINE_RECORD_COUNT),"
                "COUNT_IF(STATUS != 'SUCCEEDED'),"
                "COUNT_IF(FILE_SHA256 IS NULL OR LENGTH(FILE_SHA256) != 64 "
                "OR FILE_SIZE_BYTES <= 0) "
                "FROM SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCH_FILES "
                "WHERE BATCH_ID=%s",
                (batch_id,),
            )
            file_audit = cursor.fetchone()
            if (
                file_audit[0] != len(TABLE_COLUMNS)
                or tuple(file_audit[1:9]) != tuple(row[1:9])
                or file_audit[9]
                or file_audit[10]
            ):
                errors.append(f"batch {batch_id}: file audits do not reconcile")

        expected_total = sum(expected.values())
        first = batch_metrics.get(args.first_batch)
        rerun = batch_metrics.get(args.rerun_batch)
        correction = batch_metrics.get(args.correction_batch)
        stable = batch_metrics.get(args.stable_batch)
        if first and (
            first[0] not in {"SUCCEEDED", "SUCCESS_WITH_QUARANTINE"}
            or first[1] != expected_total
            or first[2] != expected_total
            or first[3] != expected_total
            or first[4] != 0
            or first[5] != 0
        ):
            errors.append("first batch audit does not reconcile")
        if rerun and (
            rerun[0] not in {"SUCCEEDED", "SUCCESS_WITH_QUARANTINE"}
            or rerun[1] != expected_total
            or rerun[2] != expected_total
            or rerun[3] != 0
            or rerun[4] != 0
            or rerun[5] != expected_total
        ):
            errors.append("rerun batch is not idempotent")
        corrected_rows = defects["out_of_range_discount"]
        expected_quarantine = sum(count for _, count in expected_quality.values())
        if correction and (
            correction[0] not in {"SUCCEEDED", "SUCCESS_WITH_QUARANTINE"}
            or correction[1] != expected_total
            or correction[2] != expected_total
            or correction[3] != 0
            or correction[4] != corrected_rows
            or correction[5] != expected_total - corrected_rows
            or correction[7] != expected_quarantine
            or correction[8] != expected_quarantine
        ):
            errors.append("correction batch does not reconcile")
        if stable and (
            stable[0] not in {"SUCCEEDED", "SUCCESS_WITH_QUARANTINE"}
            or stable[1] != expected_total
            or stable[2] != expected_total
            or stable[3] != 0
            or stable[4] != 0
            or stable[5] != expected_total
            or stable[7] != expected_quarantine
            or stable[8] != expected_quarantine
        ):
            errors.append("stable rerun batch is not idempotent")
    finally:
        cursor.close()
        connection.close()

    if errors:
        for error in errors:
            print(f"FAIL: {error}")
        return 1
    print("RAW reconciliation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
