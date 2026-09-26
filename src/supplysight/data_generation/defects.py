"""Documented low-rate, reproducible data-quality defects."""

from __future__ import annotations

import random
from typing import Any

DEFECTS = {
    "missing_optional_attribute": "Some product subcategories are blank.",
    "missing_delivery_timestamp": "Some delivered shipment records omit delivery_date.",
    "nonstandard_region_case": "Some customer regions use inconsistent capitalization.",
    "out_of_range_discount": "Some order discounts exceed the valid 0 to 1 range.",
    "inventory_balance_mismatch": (
        "Some available units disagree with on-hand less reserved units."
    ),
}


def inject_defects(
    tables: dict[str, list[dict[str, Any]]], *, seed: int, rate: float, clean: bool
) -> dict[str, int]:
    """Inject defects without changing primary or foreign keys."""
    counts = {name: 0 for name in DEFECTS}
    if clean or rate <= 0:
        return counts
    candidates = {
        "missing_optional_attribute": [
            row for row in tables["products"] if row["subcategory"]
        ],
        "missing_delivery_timestamp": [
            row for row in tables["shipments"] if row["shipment_status"] == "delivered"
        ],
        "nonstandard_region_case": list(tables["customers"]),
        "out_of_range_discount": list(tables["orders"]),
    }
    for index, (name, rows) in enumerate(candidates.items()):
        if not rows:
            continue
        count = min(len(rows), max(1, round(rate * len(rows))))
        rng = random.Random(seed ^ (0x5A17D3 + index * 7919))
        for row in rng.sample(rows, count):
            if name == "missing_optional_attribute":
                row["subcategory"] = ""
            elif name == "missing_delivery_timestamp":
                row["delivery_date"] = ""
            elif name == "nonstandard_region_case":
                row["region"] = row["region"].upper()
            else:
                row["discount_pct"] = "1.20"
        counts[name] = count
    return counts


def inject_inventory_mismatches(path, *, seed: int, rate: float, clean: bool) -> int:
    """Update a deterministic sample of inventory rows without loading the file."""
    if clean or rate <= 0:
        return 0
    import csv
    from pathlib import Path

    source = Path(path)
    with source.open(encoding="utf-8") as stream:
        row_count = max(0, sum(1 for _ in stream) - 1)
    if not row_count:
        return 0
    count = min(row_count, max(1, round(rate * row_count)))
    selected = set(random.Random(seed ^ 0x7163B1).sample(range(row_count), count))
    temporary = source.with_suffix(source.suffix + ".tmp")
    with (
        source.open(newline="", encoding="utf-8") as inp,
        temporary.open("w", newline="", encoding="utf-8") as out,
    ):
        reader = csv.DictReader(inp)
        writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for index, row in enumerate(reader):
            if index in selected:
                row["available_units"] = str(int(row["available_units"]) + 3)
            writer.writerow(row)
    temporary.replace(source)
    return count
