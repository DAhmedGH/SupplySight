"""Streaming preflight checks for the nine synthetic CSV contracts."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from supplysight.data_generation.schema import TABLE_COLUMNS
from supplysight.ingestion.contracts import BUSINESS_KEYS, ENTITY_ORDER, FOREIGN_KEYS

STATUSES = {
    "delivered",
    "processing",
    "backordered",
    "partially_fulfilled",
    "cancelled",
}
PO_STATUSES = {"received", "in_transit", "open"}
RETURN_REASONS = {
    "damaged",
    "wrong_item",
    "not_as_described",
    "changed_mind",
    "late_delivery",
}
DISPOSITIONS = {"quarantine", "refurbish", "dispose"}
QUALITY_CLASSES = (
    "missing_optional_attribute",
    "missing_delivery_timestamp",
    "nonstandard_region_case",
    "out_of_range_discount",
    "inventory_balance_mismatch",
)
QUARANTINE_CLASSES = {
    "missing_delivery_timestamp",
    "out_of_range_discount",
    "inventory_balance_mismatch",
}
PLAIN_DECIMAL = re.compile(r"^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$")


class PreflightError(ValueError):
    """Raised when source files cannot safely be loaded as a consistent batch."""

    def __init__(self, message: str, sidecar: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.sidecar = sidecar or []


@dataclass
class FileManifest:
    entity: str
    path: Path
    sha256: str
    size_bytes: int
    row_count: int
    warnings: int
    warning_counts: dict[str, int]


@dataclass
class PreflightResult:
    files: dict[str, FileManifest]
    keys: dict[str, set[tuple[str, ...]]] = field(repr=False)
    warning_counts: dict[str, int]
    row_counts: dict[str, int]


def _valid_value(entity: str, col: str, value: str) -> bool:
    nullable = {
        ("products", "subcategory"),
        ("shipments", "delivery_date"),
        ("purchase_orders", "received_date"),
    }
    if value == "" and (entity, col) in nullable:
        return True
    if value == "":
        return False
    date_cols = {
        "launch_date",
        "signup_date",
        "order_date",
        "promised_delivery_date",
        "snapshot_date",
        "expected_delivery_date",
        "received_date",
        "ship_date",
        "delivery_date",
        "return_date",
    }
    int_cols = {
        "reorder_point",
        "capacity_units",
        "lead_time_days",
        "quantity",
        "on_hand_units",
        "reserved_units",
        "available_units",
        "in_transit_units",
        "ordered_quantity",
        "received_quantity",
        "shipped_quantity",
    }
    decimal_cols = {
        "unit_cost",
        "list_price",
        "lead_time_std_days",
        "on_time_rate",
        "quality_rate",
        "cost_factor",
        "unit_price",
        "discount_pct",
        "inventory_value",
        "refund_amount",
    }
    bool_cols = {"active_flag"}
    if col in date_cols:
        try:
            return date.fromisoformat(value).isoformat() == value
        except ValueError:
            return False
    if col in int_cols:
        try:
            int(value)
            return str(int(value)) == value and int(value) >= 0
        except ValueError:
            return False
    if col in {"on_time_rate", "quality_rate"}:
        return (
            _is_snowflake_decimal(value)
            and Decimal("0") <= Decimal(value) <= Decimal("1")
        )
    if col in decimal_cols:
        return _is_snowflake_decimal(value)
    if col in bool_cols:
        # CSV serialization currently emits Python's canonical capitalized bools.
        return value in {"True", "False", "true", "false"}
    if entity == "orders" and col == "status":
        return value in STATUSES
    if entity == "purchase_orders" and col == "status":
        return value in PO_STATUSES
    if entity == "returns" and col == "reason":
        return value in RETURN_REASONS
    if entity == "returns" and col == "disposition":
        return value in DISPOSITIONS
    if entity == "shipments" and col == "shipment_status":
        return value in {"delivered", "in_transit"}
    return True


def _is_snowflake_decimal(value: str) -> bool:
    """Check plain decimal text fits Snowflake NUMBER(38,18) without rounding."""
    match = PLAIN_DECIMAL.fullmatch(value)
    if match is None:
        return False
    unsigned = value.lstrip("+-")
    integer, _, fraction = unsigned.partition(".")
    significant_integer = integer.lstrip("0") or "0"
    return len(significant_integer) <= 20 and len(fraction) <= 18


def _quality_issues(entity: str, row: dict[str, str]) -> list[str]:
    issues: list[str] = []
    if entity == "products" and not row["subcategory"]:
        issues.append("missing_optional_attribute")
    if (
        entity == "shipments"
        and row["shipment_status"] == "delivered"
        and not row["delivery_date"]
    ):
        issues.append("missing_delivery_timestamp")
    if entity == "customers" and row["region"] != row["region"].title():
        issues.append("nonstandard_region_case")
    if entity == "orders" and (
        Decimal(row["discount_pct"]) < 0 or Decimal(row["discount_pct"]) > 1
    ):
        issues.append("out_of_range_discount")
    if entity == "inventory_snapshots" and int(row["available_units"]) != int(
        row["on_hand_units"]
    ) - int(row["reserved_units"]):
        issues.append("inventory_balance_mismatch")
    return issues


def preflight_dataset(input_dir: Path | str) -> PreflightResult:
    """Validate all files and relationships before opening a warehouse session.

    The five documented source anomalies are retained and labeled as warnings.
    Structural, type, domain, duplicate-key, or relationship failures cause the
    entire preflight to fail; no entity graph is loaded partially.
    """
    root = Path(input_dir)
    manifests: dict[str, FileManifest] = {}
    keys = {entity: set() for entity in ENTITY_ORDER}
    warnings: dict[str, int] = {name: 0 for name in QUALITY_CLASSES}
    errors: list[str] = []
    sidecar: list[dict[str, Any]] = []
    for entity in ENTITY_ORDER:
        path = root / f"{entity}.csv"
        if not path.is_file():
            errors.append(f"missing file: {path.name}")
            continue
        digest = hashlib.sha256()
        with path.open("rb") as raw:
            for chunk in iter(lambda: raw.read(1024 * 1024), b""):
                digest.update(chunk)
        warning_counts = {name: 0 for name in QUALITY_CLASSES}
        row_count = 0
        try:
            with path.open("r", encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream, strict=True)
                if reader.fieldnames != TABLE_COLUMNS[entity]:
                    errors.append(f"{entity}: header mismatch")
                    continue
                for line_number, row in enumerate(reader, start=2):
                    row_count += 1
                    if None in row or any(value is None for value in row.values()):
                        errors.append(f"{entity}:{line_number}: malformed column count")
                        sidecar.append(
                            {
                                "entity": entity,
                                "line": line_number,
                                "payload": row,
                                "issues": ["malformed_column_count"],
                            }
                        )
                        continue
                    row_errors = [
                        col
                        for col, value in row.items()
                        if not _valid_value(entity, col, value)
                    ]
                    business_key = tuple(
                        row.get(col, "") for col in BUSINESS_KEYS[entity]
                    )
                    if not all(business_key):
                        row_errors.append("blank_business_key")
                    elif business_key in keys[entity]:
                        row_errors.append("duplicate_business_key")
                    else:
                        keys[entity].add(business_key)
                    issues = _quality_issues(entity, row) if not row_errors else []
                    for issue in issues:
                        warning_counts[issue] += 1
                        warnings[issue] += 1
                    if row_errors:
                        sidecar.append(
                            {
                                "entity": entity,
                                "line": line_number,
                                "business_key": business_key,
                                "payload": row,
                                "issues": row_errors,
                            }
                        )
                        errors.append(
                            f"{entity}:{line_number}: invalid fields/key "
                            f"({', '.join(row_errors)})"
                        )
                # Relationships are checked after all parent keys are collected.
                if reader.line_num < 1:
                    errors.append(f"{entity}: unreadable CSV")
        except (UnicodeDecodeError, csv.Error) as exc:
            errors.append(f"{entity}: invalid UTF-8/CSV ({type(exc).__name__})")
            continue
        verification_hash = hashlib.sha256()
        with path.open("rb") as raw:
            for chunk in iter(lambda: raw.read(1024 * 1024), b""):
                verification_hash.update(chunk)
        if verification_hash.hexdigest() != digest.hexdigest():
            errors.append(f"{entity}: file changed during preflight")
            continue
        manifests[entity] = FileManifest(
            entity,
            path,
            digest.hexdigest(),
            path.stat().st_size,
            row_count,
            sum(warning_counts.values()),
            warning_counts,
        )

    if len(manifests) == len(ENTITY_ORDER):
        for entity, relations in FOREIGN_KEYS.items():
            path = root / f"{entity}.csv"
            with path.open("r", encoding="utf-8", newline="") as stream:
                for line_number, row in enumerate(csv.DictReader(stream), start=2):
                    for column, parent in relations.items():
                        if (row[column],) not in keys[parent]:
                            issue = f"invalid_reference:{column}:{parent}"
                            sidecar.append(
                                {
                                    "entity": entity,
                                    "line": line_number,
                                    "business_key": tuple(
                                        row[c] for c in BUSINESS_KEYS[entity]
                                    ),
                                    "payload": row,
                                    "issues": [issue],
                                }
                            )
                            errors.append(
                                f"{entity}:{line_number}: invalid {column} reference"
                            )
    if errors:
        raise PreflightError("Preflight failed: " + "; ".join(errors[:20]), sidecar)
    return PreflightResult(
        manifests,
        keys,
        warnings,
        {name: manifest.row_count for name, manifest in manifests.items()},
    )


def write_quarantine_sidecar(
    rows: list[dict[str, Any]], destination: Path | str
) -> Path | None:
    """Write structural rejects to a local JSONL sidecar for operator review."""
    if not rows:
        return None
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, default=list) + "\n")
    return path
