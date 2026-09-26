"""Tests for the orchestration wrapper around the established RAW pipeline."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts import orchestrate_raw
from supplysight.ingestion import PreflightError


def test_preflight_reports_manifest_counts(monkeypatch, tmp_path):
    counts = {entity: 1 for entity in orchestrate_raw.ENTITY_ORDER}
    monkeypatch.setattr(
        orchestrate_raw,
        "preflight_dataset",
        lambda _: SimpleNamespace(
            files={entity: object() for entity in counts},
            row_counts=counts,
            warning_counts={"missing_optional_attribute": 2},
        ),
    )
    assert orchestrate_raw.preflight_command(tmp_path) == {
        "status": "passed",
        "sources": 9,
        "row_counts": counts,
        "warning_rows": 2,
    }


def test_ingest_preflight_failure_stops_before_snowflake(monkeypatch, tmp_path):
    def fail(_):
        raise PreflightError("bad source")

    monkeypatch.setattr(orchestrate_raw, "preflight_dataset", fail)
    monkeypatch.setattr(
        orchestrate_raw,
        "ingest_dataset",
        lambda *args, **kwargs: pytest.fail(
            "ingest must not run after failed preflight"
        ),
    )
    with pytest.raises(PreflightError):
        orchestrate_raw.ingest_command(
            tmp_path, tmp_path / ".env", tmp_path / "result.json"
        )
    assert not (tmp_path / "result.json").exists()


def test_ingest_writes_secret_free_result_atomically(monkeypatch, tmp_path):
    counts = {entity: i + 1 for i, entity in enumerate(orchestrate_raw.ENTITY_ORDER)}
    files = {
        entity: SimpleNamespace(sha256=f"{i:064x}", size_bytes=i + 100)
        for i, entity in enumerate(orchestrate_raw.ENTITY_ORDER)
    }
    preflight = SimpleNamespace(row_counts=counts, files=files)
    settings = SimpleNamespace(
        database="SUPPLY_CHAIN_DEV", schema="RAW", warehouse="SUPPLY_CHAIN_DEV_WH"
    )
    monkeypatch.setattr(orchestrate_raw, "preflight_dataset", lambda _: preflight)
    monkeypatch.setattr(orchestrate_raw, "_settings", lambda _: settings)
    monkeypatch.setattr(
        orchestrate_raw,
        "ingest_dataset",
        lambda *args, **kwargs: SimpleNamespace(
            batch_id="batch-123",
            source_rows=sum(counts.values()),
            inserted_rows=20,
            updated_rows=1,
            unchanged_rows=2,
            warning_rows=3,
            quarantined_rows=4,
        ),
    )
    result_path = tmp_path / "runs" / "run.json"
    assert orchestrate_raw.ingest_command(tmp_path, tmp_path / ".env", result_path) == {
        "status": "succeeded",
        "batch_id": "batch-123",
    }
    data = json.loads(result_path.read_text(encoding="utf-8"))
    assert data["source_counts"] == counts
    assert data["source_files"]["products"] == {
        "sha256": files["products"].sha256,
        "size_bytes": files["products"].size_bytes,
    }
    assert data["warehouse"] == "SUPPLY_CHAIN_DEV_WH"
    assert not {"account", "user", "private_key", "passphrase"}.intersection(data)
    assert list(result_path.parent.iterdir()) == [result_path]


def test_validate_rejects_failed_ingestion_result_before_connect(monkeypatch, tmp_path):
    result_path = tmp_path / "run.json"
    result_path.write_text(json.dumps({"status": "failed"}), encoding="utf-8")
    monkeypatch.setattr(
        orchestrate_raw,
        "_connect",
        lambda *args: pytest.fail("failed ingestion must gate validation"),
    )
    with pytest.raises(ValueError, match="invalid environment"):
        orchestrate_raw.validate_command(tmp_path, tmp_path / ".env", result_path)


def test_validate_rejects_same_count_changed_input_before_connect(
    monkeypatch, tmp_path
):
    result_path = tmp_path / "run.json"
    counts = {entity: 1 for entity in orchestrate_raw.ENTITY_ORDER}
    prior_files = {
        entity: {"sha256": "a" * 64, "size_bytes": 100}
        for entity in orchestrate_raw.ENTITY_ORDER
    }
    result_path.write_text(
        json.dumps(
            {
                "status": "succeeded",
                "database": "SUPPLY_CHAIN_DEV",
                "schema": "RAW",
                "warehouse": "SUPPLY_CHAIN_DEV_WH",
                "batch_id": "batch-123",
                "source_counts": counts,
                "source_files": prior_files,
            }
        ),
        encoding="utf-8",
    )
    changed_files = {
        entity: SimpleNamespace(sha256="b" * 64, size_bytes=100)
        for entity in orchestrate_raw.ENTITY_ORDER
    }
    monkeypatch.setattr(
        orchestrate_raw,
        "preflight_dataset",
        lambda _: SimpleNamespace(row_counts=counts, files=changed_files),
    )
    monkeypatch.setattr(
        orchestrate_raw,
        "_connect",
        lambda *args: pytest.fail("changed inputs must be rejected before Snowflake"),
    )
    with pytest.raises(ValueError, match="hashes or sizes"):
        orchestrate_raw.validate_command(tmp_path, tmp_path / ".env", result_path)


def test_validate_rejects_a_missing_source_audit(monkeypatch, tmp_path):
    counts = {entity: 1 for entity in orchestrate_raw.ENTITY_ORDER}
    files = {
        entity: {"sha256": f"{i:064x}", "size_bytes": i + 100}
        for i, entity in enumerate(orchestrate_raw.ENTITY_ORDER)
    }
    result_path = tmp_path / "run.json"
    result_path.write_text(
        json.dumps(
            {
                "status": "succeeded",
                "database": "SUPPLY_CHAIN_DEV",
                "schema": "RAW",
                "warehouse": "SUPPLY_CHAIN_DEV_WH",
                "batch_id": "batch-123",
                "source_counts": counts,
                "source_files": files,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        orchestrate_raw,
        "preflight_dataset",
        lambda _: SimpleNamespace(
            row_counts=counts,
            files={
                entity: SimpleNamespace(**files[entity])
                for entity in orchestrate_raw.ENTITY_ORDER
            },
        ),
    )
    monkeypatch.setattr(orchestrate_raw, "_settings", lambda _: object())

    class Cursor:
        def __init__(self):
            self.sql = ""

        def execute(self, sql, params=None):
            self.sql = sql

        def fetchone(self):
            if "INGESTION_BATCHES" in self.sql:
                return "SUCCEEDED", 9, 9
            if "COUNT_IF" in self.sql:
                return (0,)
            return (0,)

        def fetchall(self):
            return [
                (entity.upper(), 1, "SUCCEEDED", "a" * 64, 10)
                for entity in orchestrate_raw.ENTITY_ORDER[:-1]
            ]

        def close(self):
            pass

    cursor = Cursor()

    class Connection:
        def cursor(self):
            return cursor

        def close(self):
            pass

    monkeypatch.setattr(orchestrate_raw, "_connect", lambda *args: Connection())
    monkeypatch.setattr(orchestrate_raw, "_verify_session", lambda *args: None)
    with pytest.raises(ValueError, match="exactly one audit"):
        orchestrate_raw.validate_command(tmp_path, tmp_path / ".env", result_path)


def test_validate_rejects_snowflake_file_hash_mismatch(monkeypatch, tmp_path):
    counts = {entity: 1 for entity in orchestrate_raw.ENTITY_ORDER}
    files = {
        entity: {"sha256": f"{i:064x}", "size_bytes": i + 100}
        for i, entity in enumerate(orchestrate_raw.ENTITY_ORDER)
    }
    result_path = tmp_path / "run.json"
    result_path.write_text(
        json.dumps(
            {
                "status": "succeeded",
                "database": "SUPPLY_CHAIN_DEV",
                "schema": "RAW",
                "warehouse": "SUPPLY_CHAIN_DEV_WH",
                "batch_id": "batch-123",
                "source_counts": counts,
                "source_files": files,
            }
        ),
        encoding="utf-8",
    )
    manifests = {
        entity: SimpleNamespace(**files[entity])
        for entity in orchestrate_raw.ENTITY_ORDER
    }
    monkeypatch.setattr(
        orchestrate_raw,
        "preflight_dataset",
        lambda _: SimpleNamespace(row_counts=counts, files=manifests),
    )
    monkeypatch.setattr(orchestrate_raw, "_settings", lambda _: object())

    class Cursor:
        sql = ""

        def execute(self, sql, params=None):
            self.sql = sql

        def fetchone(self):
            if "INGESTION_BATCHES" in self.sql:
                return "SUCCEEDED", 9, 9
            return (0,)

        def fetchall(self):
            return [
                (
                    entity.upper(),
                    1,
                    "SUCCEEDED",
                    "f" * 64 if entity == "products" else files[entity]["sha256"],
                    files[entity]["size_bytes"],
                )
                for entity in orchestrate_raw.ENTITY_ORDER
            ]

        def close(self):
            pass

    cursor = Cursor()

    class Connection:
        def cursor(self):
            return cursor

        def close(self):
            pass

    monkeypatch.setattr(orchestrate_raw, "_connect", lambda *args: Connection())
    monkeypatch.setattr(orchestrate_raw, "_verify_session", lambda *args: None)
    with pytest.raises(ValueError, match="products: file audit does not reconcile"):
        orchestrate_raw.validate_command(tmp_path, tmp_path / ".env", result_path)
