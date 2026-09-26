"""Airflow-friendly commands for preflighting, loading, and auditing RAW data."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from supplysight.data_generation.schema import TABLE_COLUMNS
from supplysight.ingestion import PreflightError, ingest_dataset, preflight_dataset
from supplysight.ingestion.contracts import BUSINESS_KEYS, ENTITY_ORDER, SOURCE_SYSTEM
from supplysight.ingestion.pipeline import _connect, _verify_session
from supplysight.settings import ConfigurationError, SnowflakeSettings


def _settings(env_file: Path) -> SnowflakeSettings:
    load_dotenv(env_file, override=False)
    settings = SnowflakeSettings.from_environment()
    if (settings.database, settings.schema, settings.warehouse) != (
        "SUPPLY_CHAIN_DEV",
        "RAW",
        "SUPPLY_CHAIN_DEV_WH",
    ):
        raise ConfigurationError("RAW orchestration is restricted to the DEV target.")
    return settings


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            temporary = stream.name
            json.dump(payload, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def preflight_command(input_dir: Path) -> dict[str, Any]:
    result = preflight_dataset(input_dir)
    return {
        "status": "passed",
        "sources": len(result.files),
        "row_counts": {key: result.row_counts[key] for key in ENTITY_ORDER},
        "warning_rows": sum(result.warning_counts.values()),
    }


def ingest_command(
    input_dir: Path, env_file: Path, result_path: Path
) -> dict[str, Any]:
    validation = preflight_dataset(input_dir)
    settings = _settings(env_file)
    result = ingest_dataset(input_dir, settings, preflight=validation)
    payload = {
        "batch_id": result.batch_id,
        "status": "succeeded",
        "database": settings.database,
        "schema": settings.schema,
        "warehouse": settings.warehouse,
        "source_counts": {key: validation.row_counts[key] for key in ENTITY_ORDER},
        "source_files": {
            key: {
                "sha256": validation.files[key].sha256,
                "size_bytes": validation.files[key].size_bytes,
            }
            for key in ENTITY_ORDER
        },
        "source_rows": result.source_rows,
        "inserted_rows": result.inserted_rows,
        "updated_rows": result.updated_rows,
        "unchanged_rows": result.unchanged_rows,
        "warning_rows": result.warning_rows,
        "quarantined_rows": result.quarantined_rows,
    }
    _atomic_json(result_path, payload)
    return {"status": payload["status"], "batch_id": result.batch_id}


def validate_command(
    input_dir: Path, env_file: Path, result_path: Path
) -> dict[str, Any]:
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    if (
        payload.get("status") != "succeeded"
        or payload.get("database") != "SUPPLY_CHAIN_DEV"
        or payload.get("schema") != "RAW"
        or payload.get("warehouse") != "SUPPLY_CHAIN_DEV_WH"
        or not payload.get("batch_id")
    ):
        raise ValueError(
            "Ingestion result is incomplete or targets an invalid environment."
        )
    current = preflight_dataset(input_dir)
    expected = current.row_counts
    if payload.get("source_counts") != {key: expected[key] for key in ENTITY_ORDER}:
        raise ValueError(
            "Ingestion result source counts do not match current input files."
        )
    expected_files = {
        key: {
            "sha256": current.files[key].sha256,
            "size_bytes": current.files[key].size_bytes,
        }
        for key in ENTITY_ORDER
    }
    if payload.get("source_files") != expected_files:
        raise ValueError(
            "Ingestion result file hashes or sizes do not match current input files."
        )

    settings = _settings(env_file)
    connection = _connect(settings, None)
    cursor = connection.cursor()
    errors: list[str] = []
    try:
        _verify_session(cursor, settings)
        cursor.execute(
            "SELECT STATUS,SOURCE_ROW_COUNT,MERGED_ROW_COUNT FROM "
            "SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCHES WHERE BATCH_ID=%s",
            (payload["batch_id"],),
        )
        batch = cursor.fetchone()
        if batch is None or batch[0] not in {"SUCCEEDED", "SUCCESS_WITH_QUARANTINE"}:
            errors.append("batch audit is missing or unsuccessful")
        elif batch[1] != sum(expected.values()) or batch[2] != sum(expected.values()):
            errors.append("batch audit row totals do not match source files")

        cursor.execute(
            "SELECT ENTITY_NAME,SOURCE_ROW_COUNT,STATUS,FILE_SHA256,FILE_SIZE_BYTES "
            "FROM SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCH_FILES WHERE BATCH_ID=%s",
            (payload["batch_id"],),
        )
        audits = cursor.fetchall()
        by_entity = {str(row[0]).lower(): row for row in audits}
        if len(audits) != len(ENTITY_ORDER) or set(by_entity) != set(ENTITY_ORDER):
            errors.append(
                "batch does not have exactly one audit for each of nine sources"
            )
        for entity in ENTITY_ORDER:
            row = by_entity.get(entity)
            if row and (
                row[1] != expected[entity]
                or row[2] != "SUCCEEDED"
                or str(row[3]).lower() != expected_files[entity]["sha256"].lower()
                or row[4] != expected_files[entity]["size_bytes"]
            ):
                errors.append(f"{entity}: file audit does not reconcile")

        for entity in ENTITY_ORDER:
            cursor.execute(
                "SELECT COUNT_IF(_INGESTED_AT IS NULL OR _SOURCE_FILE IS NULL OR "
                "_BATCH_ID IS NULL OR _ROW_HASH IS NULL OR _SOURCE_SYSTEM != %s) "
                f"FROM SUPPLY_CHAIN_DEV.RAW.{entity.upper()}",
                (SOURCE_SYSTEM,),
            )
            invalid_metadata = cursor.fetchone()[0] or 0
            if invalid_metadata:
                errors.append(f"{entity}: invalid ingestion metadata")
            keys = ",".join(key.upper() for key in BUSINESS_KEYS[entity])
            cursor.execute(
                f"SELECT COUNT(*) FROM (SELECT {keys} FROM SUPPLY_CHAIN_DEV.RAW."
                f"{entity.upper()} GROUP BY {keys} HAVING COUNT(*) > 1)"
            )
            if cursor.fetchone()[0]:
                errors.append(f"{entity}: duplicate business keys")
    finally:
        cursor.close()
        connection.close()

    if errors:
        raise ValueError("RAW validation failed: " + "; ".join(errors))
    return {
        "status": "passed",
        "batch_id": payload["batch_id"],
        "sources": len(TABLE_COLUMNS),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Airflow interface for the existing RAW pipeline."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("preflight", "ingest", "validate"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--input-dir", type=Path, default=Path("data/generated/dev"))
        if command != "preflight":
            sub.add_argument("--env-file", type=Path, default=Path(".env"))
        if command != "preflight":
            sub.add_argument("--result-path", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "preflight":
            output = preflight_command(args.input_dir)
        elif args.command == "ingest":
            output = ingest_command(args.input_dir, args.env_file, args.result_path)
        else:
            output = validate_command(args.input_dir, args.env_file, args.result_path)
    except Exception as exc:
        # Do not expose connector details, which may contain account information.
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(exc)
                    if isinstance(exc, (ValueError, PreflightError))
                    else type(exc).__name__,
                }
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
