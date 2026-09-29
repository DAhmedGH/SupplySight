"""Streaming structural, relationship, and inventory movement checks."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from supplysight.data_generation.schema import TABLE_COLUMNS

KEYS = {
    "products": "product_id",
    "warehouses": "warehouse_id",
    "suppliers": "supplier_id",
    "customers": "customer_id",
    "orders": "order_line_id",
    "purchase_orders": "po_line_id",
    "shipments": "shipment_id",
    "returns": "return_id",
}
RETAIN_ROWS = {
    "products",
    "warehouses",
    "customers",
    "orders",
    "purchase_orders",
    "shipments",
    "returns",
}


def validate_dataset(
    output_dir: Path, *, as_of_date: date | None = None
) -> dict[str, Any]:
    """Validate keys, relationships, status chronology, and inventory movements."""
    errors: list[str] = []
    counts: dict[str, int] = {}
    keys: dict[str, set[str]] = {}
    rows: dict[str, list[dict[str, str]]] = {}
    snapshot_keys: set[tuple[str, str, str]] = set()
    for table, columns in TABLE_COLUMNS.items():
        path = output_dir / f"{table}.csv"
        if not path.is_file():
            errors.append(f"missing file: {path.name}")
            continue
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != columns:
                errors.append(f"{table}: unexpected columns or column order")
            retained: list[dict[str, str]] = []
            key = KEYS.get(table)
            values: set[str] = set()
            duplicate = False
            for row in reader:
                if table in RETAIN_ROWS:
                    retained.append(row)
                if key:
                    value = row.get(key, "")
                    if not value:
                        errors.append(f"{table}: blank {key}")
                    if value in values:
                        duplicate = True
                    values.add(value)
                if table == "inventory_snapshots":
                    composite = (
                        row["snapshot_date"],
                        row["warehouse_id"],
                        row["product_id"],
                    )
                    if composite in snapshot_keys:
                        errors.append(
                            "inventory_snapshots: duplicate date, warehouse, "
                            "product key"
                        )
                    snapshot_keys.add(composite)
            counts[table] = reader.line_num - 1
        if key:
            if duplicate:
                errors.append(f"{table}: duplicate {key}")
            keys[table] = values
        if table in RETAIN_ROWS:
            rows[table] = retained

    def fk(table: str, column: str, parent: str) -> None:
        path = output_dir / f"{table}.csv"
        if not path.is_file() or parent not in keys:
            return
        with path.open(newline="", encoding="utf-8") as stream:
            missing = sum(
                row.get(column, "") not in keys[parent]
                for row in csv.DictReader(stream)
            )
        if missing:
            errors.append(f"{table}: {missing} invalid {column} references")

    for table in ("orders", "inventory_snapshots", "purchase_orders"):
        fk(table, "product_id", "products")
        fk(table, "warehouse_id", "warehouses")
    fk("orders", "customer_id", "customers")
    fk("purchase_orders", "supplier_id", "suppliers")
    fk("shipments", "order_line_id", "orders")
    fk("returns", "order_line_id", "orders")

    orders = {row["order_line_id"]: row for row in rows.get("orders", [])}
    customers = {row["customer_id"]: row for row in rows.get("customers", [])}
    for order in rows.get("orders", []):
        customer = customers.get(order["customer_id"])
        if customer and customer["signup_date"] > order["order_date"]:
            errors.append(f"order {order['order_id']}: customer signup after order")

    shipments_by_line: dict[str, dict[str, str]] = {}
    shipped_by_line: defaultdict[str, int] = defaultdict(int)
    movements: defaultdict[tuple[str, str], list[tuple[date, int]]] = defaultdict(list)
    transit_movements: defaultdict[tuple[str, str], list[tuple[date, int]]] = (
        defaultdict(list)
    )
    for shipment in rows.get("shipments", []):
        line_id = shipment["order_line_id"]
        order = orders.get(line_id)
        shipments_by_line[line_id] = shipment
        shipped_qty = int(shipment["shipped_quantity"])
        shipped_by_line[line_id] += shipped_qty
        if order:
            if shipment["ship_date"] < order["order_date"]:
                errors.append(f"shipment {shipment['shipment_id']}: ships before order")
            if shipment["warehouse_id"] != order["warehouse_id"]:
                errors.append(
                    f"shipment {shipment['shipment_id']}: warehouse differs from order"
                )
            if shipped_qty > int(order["quantity"]):
                errors.append(
                    f"shipment {shipment['shipment_id']}: exceeds ordered quantity"
                )
            movements[(shipment["warehouse_id"], order["product_id"])].append(
                (date.fromisoformat(shipment["ship_date"]), -shipped_qty)
            )
        if (
            shipment["delivery_date"]
            and shipment["delivery_date"] < shipment["ship_date"]
        ):
            errors.append(
                f"shipment {shipment['shipment_id']}: delivery before ship date"
            )
        if as_of_date and date.fromisoformat(shipment["ship_date"]) > as_of_date:
            errors.append(f"shipment {shipment['shipment_id']}: ships after as-of date")
        if (
            as_of_date
            and shipment["delivery_date"]
            and date.fromisoformat(shipment["delivery_date"]) > as_of_date
        ):
            errors.append(
                f"shipment {shipment['shipment_id']}: delivery after as-of date"
            )
        if shipment["shipment_status"] == "in_transit" and shipment["delivery_date"]:
            errors.append(
                f"shipment {shipment['shipment_id']}: in-transit delivery is realized"
            )
        if shipped_qty < 1:
            errors.append(f"shipment {shipment['shipment_id']}: nonpositive quantity")

    for line_id, order in orders.items():
        if as_of_date and date.fromisoformat(order["order_date"]) > as_of_date:
            errors.append(f"order line {line_id}: ordered after as-of date")
        quantity = int(order["quantity"])
        shipped = shipped_by_line[line_id]
        status = order["status"]
        if shipped > quantity:
            errors.append(f"order line {line_id}: shipped quantity exceeds ordered")
        if status == "cancelled" and shipped:
            errors.append(f"order line {line_id}: cancelled line has shipment")
        elif status == "backordered" and shipped:
            errors.append(f"order line {line_id}: backorder has shipment")
        elif status == "partially_fulfilled" and not 0 < shipped < quantity:
            errors.append(f"order line {line_id}: invalid partial fulfillment quantity")
        elif status in {"delivered", "processing"} and shipped != quantity:
            errors.append(
                f"order line {line_id}: full-fulfillment status quantity mismatch"
            )
        elif status not in {
            "cancelled",
            "backordered",
            "partially_fulfilled",
            "delivered",
            "processing",
        }:
            errors.append(f"order line {line_id}: unsupported status {status}")
        shipment = shipments_by_line.get(line_id)
        if (
            status == "delivered"
            and shipment
            and shipment["shipment_status"] != "delivered"
        ):
            errors.append(
                f"order line {line_id}: delivered order lacks delivered shipment"
            )
        if (
            status == "processing"
            and shipment
            and shipment["shipment_status"] != "in_transit"
        ):
            errors.append(f"order line {line_id}: processing order status mismatch")

    for ret in rows.get("returns", []):
        line_id = ret["order_line_id"]
        order = orders.get(line_id)
        shipment = shipments_by_line.get(line_id)
        quantity = int(ret["quantity"])
        if as_of_date and date.fromisoformat(ret["return_date"]) > as_of_date:
            errors.append(f"return {ret['return_id']}: after as-of date")
        if (
            shipment
            and shipment["delivery_date"]
            and ret["return_date"] < shipment["delivery_date"]
        ):
            errors.append(f"return {ret['return_id']}: return before delivery")
        if quantity < 1 or float(ret["refund_amount"]) < 0:
            errors.append(f"return {ret['return_id']}: invalid quantity or refund")
        if order and quantity > shipped_by_line[line_id]:
            errors.append(f"return {ret['return_id']}: exceeds shipped quantity")
        if shipment and shipment["shipment_status"] != "delivered":
            errors.append(f"return {ret['return_id']}: shipment is not delivered")
        if ret["disposition"] not in {"quarantine", "refurbish", "dispose"}:
            errors.append(f"return {ret['return_id']}: unsupported disposition")

    for po in rows.get("purchase_orders", []):
        expected = date.fromisoformat(po["expected_delivery_date"])
        ordered = date.fromisoformat(po["order_date"])
        if as_of_date and ordered > as_of_date:
            errors.append(f"purchase order {po['purchase_order_id']}: after as-of date")
        received = (
            date.fromisoformat(po["received_date"]) if po["received_date"] else None
        )
        qty = int(po["received_quantity"])
        if expected < ordered:
            errors.append(
                f"purchase order {po['purchase_order_id']}: expected before order"
            )
        if received and received < ordered:
            errors.append(
                f"purchase order {po['purchase_order_id']}: received before order"
            )
        if qty > int(po["ordered_quantity"]):
            errors.append(
                f"purchase order {po['purchase_order_id']}: received exceeds ordered"
            )
        if po["status"] == "received":
            if not received or qty < 1 or (as_of_date and received > as_of_date):
                errors.append(
                    f"purchase order {po['purchase_order_id']}: invalid received status"
                )
            if received:
                key = (po["warehouse_id"], po["product_id"])
                movements[key].append((received, qty))
                transit_movements[key].append((ordered, int(po["ordered_quantity"])))
                transit_movements[key].append((received, -int(po["ordered_quantity"])))
        elif po["status"] in {"open", "in_transit"}:
            if received or qty != 0:
                errors.append(
                    f"purchase order {po['purchase_order_id']}: unreceived "
                    "status has receipt values"
                )
            if po["status"] == "in_transit":
                transit_movements[(po["warehouse_id"], po["product_id"])].append(
                    (ordered, int(po["ordered_quantity"]))
                )
        else:
            errors.append(
                f"purchase order {po['purchase_order_id']}: unsupported status"
            )

    movement_events = {key: sorted(events) for key, events in movements.items()}
    movement_indices: defaultdict[tuple[str, str], int] = defaultdict(int)
    previous: dict[tuple[str, str], tuple[date, int]] = {}
    transit_events = {key: sorted(events) for key, events in transit_movements.items()}
    transit_indices: defaultdict[tuple[str, str], int] = defaultdict(int)
    transit_balance: Counter[tuple[str, str]] = Counter()
    with (output_dir / "inventory_snapshots.csv").open(
        newline="", encoding="utf-8"
    ) as stream:
        for snapshot in csv.DictReader(stream):
            key = (snapshot["warehouse_id"], snapshot["product_id"])
            snapshot_date = date.fromisoformat(snapshot["snapshot_date"])
            on_hand = int(snapshot["on_hand_units"])
            reserved = int(snapshot["reserved_units"])
            available = int(snapshot["available_units"])
            if on_hand < 0 or reserved < 0 or reserved > on_hand:
                errors.append(
                    "inventory snapshot has invalid on-hand or reserved units"
                )
            if available < 0:
                errors.append("inventory snapshot has negative available units")
            prior = previous.get(key)
            event_list = movement_events.get(key, [])
            index = movement_indices[key]
            movement = 0
            while index < len(event_list) and event_list[index][0] <= snapshot_date:
                movement += event_list[index][1]
                index += 1
            movement_indices[key] = index
            transit_list = transit_events.get(key, [])
            transit_index = transit_indices[key]
            while (
                transit_index < len(transit_list)
                and transit_list[transit_index][0] <= snapshot_date
            ):
                transit_balance[key] += transit_list[transit_index][1]
                transit_index += 1
            transit_indices[key] = transit_index
            if transit_balance[key] != int(snapshot["in_transit_units"]):
                errors.append(
                    f"inventory {key} on {snapshot['snapshot_date']}: in-transit "
                    "units do not reconcile to purchase orders"
                )
            if prior is None:
                inferred_opening = on_hand - movement
                if inferred_opening < 0:
                    errors.append(
                        f"inventory {key}: fulfilled movement exceeds opening stock"
                    )
            else:
                prior_date, prior_balance = prior
                if snapshot_date <= prior_date:
                    errors.append(f"inventory {key}: snapshots are not date ordered")
                expected_balance = prior_balance + movement
                if expected_balance < 0 or expected_balance != on_hand:
                    errors.append(
                        f"inventory {key} on {snapshot['snapshot_date']}: balance "
                        "does not reconcile to receipts and shipments"
                    )
            previous[key] = (snapshot_date, on_hand)

    findings = {
        "missing_optional_attribute": _count_if(
            output_dir / "products.csv", lambda row: not row["subcategory"]
        ),
        "missing_delivery_timestamp": _count_if(
            output_dir / "shipments.csv",
            lambda row: (
                row["shipment_status"] == "delivered" and not row["delivery_date"]
            ),
        ),
        "nonstandard_region_case": _count_if(
            output_dir / "customers.csv",
            lambda row: row["region"] != row["region"].title(),
        ),
        "out_of_range_discount": _count_if(
            output_dir / "orders.csv",
            lambda row: not 0 <= float(row["discount_pct"]) <= 1,
        ),
        "inventory_balance_mismatch": _count_if(
            output_dir / "inventory_snapshots.csv",
            lambda row: (
                int(row["available_units"])
                != int(row["on_hand_units"]) - int(row["reserved_units"])
            ),
        ),
    }
    business_patterns = {
        "supplier_late_receipts": _count_if(
            output_dir / "purchase_orders.csv",
            lambda row: (
                bool(row["received_date"])
                and row["received_date"] > row["expected_delivery_date"]
            ),
        )
    }
    return {
        "valid": not errors,
        "row_counts": counts,
        "errors": errors,
        "data_quality_findings": findings,
        "business_patterns": business_patterns,
    }


def _count_if(path: Path, predicate: Callable[[dict[str, str]], bool]) -> int:
    if not path.is_file():
        return 0
    with path.open(newline="", encoding="utf-8") as stream:
        return sum(1 for row in csv.DictReader(stream) if predicate(row))
