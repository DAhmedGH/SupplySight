"""Generate linked, reproducible operational CSVs without external data sources."""

from __future__ import annotations

import csv
import json
import random
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

from supplysight.data_generation.config import GenerationConfig
from supplysight.data_generation.defects import (
    DEFECTS,
    inject_defects,
    inject_inventory_mismatches,
)
from supplysight.data_generation.schema import TABLE_COLUMNS
from supplysight.data_generation.validation import validate_dataset

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
REGION_COUNTRIES = {r: "US" for r in REGIONS}
CATEGORIES = ["Electronics", "Home", "Outdoor", "Office", "Apparel"]
SUBCATEGORIES = {
    "Electronics": ["Audio", "Accessories", "Networking"],
    "Home": ["Kitchen", "Storage", "Lighting"],
    "Outdoor": ["Camping", "Garden", "Fitness"],
    "Office": ["Paper", "Writing", "Organization"],
    "Apparel": ["Basics", "Outerwear", "Accessories"],
}


def generate_dataset(
    config: GenerationConfig, *, clean: bool = False
) -> dict[str, Any]:
    """Create all Phase 2 CSV files and a machine-readable generation report."""
    config.output_dir.mkdir(parents=True, exist_ok=True)
    rngs = {
        name: random.Random(config.seed + i * 104729)
        for i, name in enumerate(TABLE_COLUMNS)
    }
    masters = _make_masters(config, rngs)
    products, warehouses, suppliers, customers = (
        masters[k] for k in ("products", "warehouses", "suppliers", "customers")
    )

    # Date, category, region, and SKU weights shape order-line demand.
    order_rng = rngs["orders"]
    customers_by_region = _group(customers, "region")
    warehouses_by_region = _group(warehouses, "region")
    products_by_category = _group(products, "category")
    product_weights = {
        category: _product_weights(rows)
        for category, rows in products_by_category.items()
    }
    fallback_product_weights = _product_weights(products)
    dates = _date_weights(config)
    orders: list[dict[str, Any]] = []
    order_line_keys: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    order_lines_by_day: defaultdict[tuple[date, str, str], list[dict[str, Any]]] = (
        defaultdict(list)
    )
    i = 0
    order_num = 0
    while i < config.order_line_count:
        order_num += 1
        line_count = min(
            order_rng.choices([1, 2, 3, 4], [0.38, 0.38, 0.18, 0.06])[0],
            config.order_line_count - i,
        )
        order_day = order_rng.choices(dates[0], weights=dates[1], k=1)[0]
        region = order_rng.choices(
            REGIONS, weights=[0.29, 0.23, 0.19, 0.12, 0.17], k=1
        )[0]
        customer_pool = customers_by_region.get(region) or customers
        customer = order_rng.choice(customer_pool)
        wh_pool = warehouses_by_region.get(region) or warehouses
        warehouse = order_rng.choice(wh_pool)
        order_id = f"ORD-{order_num:08d}"
        promise = order_day + timedelta(days=order_rng.randint(3, 8))
        status = order_rng.choices(
            ["delivered", "processing", "cancelled"], [0.92, 0.055, 0.025]
        )[0]
        for line_no in range(1, line_count + 1):
            q = (order_day.month - 1) // 3 + 1
            category_weights = _category_weights(q)
            category = order_rng.choices(CATEGORIES, weights=category_weights, k=1)[0]
            product_group = products_by_category.get(category) or products
            product = order_rng.choices(
                product_group,
                weights=product_weights.get(category, fallback_product_weights),
                k=1,
            )[0]
            qty = order_rng.choices(
                [1, 2, 3, 4, 5, 8, 12], [0.43, 0.25, 0.13, 0.08, 0.06, 0.035, 0.015]
            )[0]
            discount = order_rng.choices(
                [0, 0.05, 0.10, 0.15, 0.20], [0.52, 0.18, 0.18, 0.09, 0.03]
            )[0]
            row = {
                "order_id": order_id,
                "order_line_id": f"{order_id}-L{line_no:02d}",
                "order_date": order_day.isoformat(),
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "warehouse_id": warehouse["warehouse_id"],
                "quantity": qty,
                "unit_price": product["list_price"],
                "discount_pct": discount,
                "status": status,
                "promised_delivery_date": promise.isoformat(),
            }
            orders.append(row)
            order_line_keys.append((row, product, warehouse))
            if status != "cancelled":
                demand_key = (
                    order_day,
                    warehouse["warehouse_id"],
                    product["product_id"],
                )
                order_lines_by_day[demand_key].append(row)
            i += 1
    po_rows, shipment_allocations = _simulate_inventory(
        config,
        products,
        warehouses,
        suppliers,
        order_lines_by_day,
        rngs["purchase_orders"],
    )
    shipment_rows, return_rows = _make_fulfillment(
        config,
        order_line_keys,
        shipment_allocations,
        rngs["shipments"],
        rngs["returns"],
    )
    tables = {
        "products": products,
        "customers": customers,
        "purchase_orders": po_rows,
        "shipments": shipment_rows,
        "orders": orders,
    }
    defect_counts = inject_defects(
        tables, seed=config.seed, rate=config.defect_rate, clean=clean
    )
    defect_counts["inventory_balance_mismatch"] = inject_inventory_mismatches(
        config.output_dir / "inventory_snapshots.csv",
        seed=config.seed,
        rate=config.defect_rate,
        clean=clean,
    )
    _write_csv(config.output_dir / "products.csv", "products", products)
    _write_csv(config.output_dir / "warehouses.csv", "warehouses", warehouses)
    _write_csv(config.output_dir / "suppliers.csv", "suppliers", suppliers)
    _write_csv(config.output_dir / "customers.csv", "customers", customers)
    _write_csv(config.output_dir / "orders.csv", "orders", orders)
    _write_csv(config.output_dir / "purchase_orders.csv", "purchase_orders", po_rows)
    _write_csv(config.output_dir / "shipments.csv", "shipments", shipment_rows)
    _write_csv(config.output_dir / "returns.csv", "returns", return_rows)
    validation = validate_dataset(config.output_dir, as_of_date=config.end_date)
    observed = validation["data_quality_findings"]
    validation["injected_defect_counts_match"] = {
        name: observed[name] == count for name, count in defect_counts.items()
    }
    if not all(validation["injected_defect_counts_match"].values()):
        raise ValueError("generated data is missing an expected injected defect")
    report = {
        "profile": config.profile,
        "seed": config.seed,
        "period": {
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
        },
        "clean": clean,
        "configured_defect_rate": 0 if clean else config.defect_rate,
        "defect_definitions": DEFECTS,
        "defect_counts": defect_counts,
        "row_counts": {
            name: _count_csv(config.output_dir / f"{name}.csv")
            for name in TABLE_COLUMNS
        },
        "validation": validation,
    }
    (config.output_dir / "generation_report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    if not validation["valid"]:
        raise ValueError(f"generated data failed validation: {validation['errors']}")
    return report


def _make_masters(
    config: GenerationConfig, rngs: dict[str, random.Random]
) -> dict[str, list[dict[str, Any]]]:
    prng, wrng, srng, crng = (
        rngs[k] for k in ("products", "warehouses", "suppliers", "customers")
    )
    suppliers = []
    for i in range(config.supplier_count):
        suppliers.append(
            {
                "supplier_id": f"SUP-{i + 1:04d}",
                "supplier_name": f"Supplier {i + 1:03d}",
                "region": REGIONS[i % len(REGIONS)],
                "country": "US",
                "lead_time_days": srng.randint(5, 30),
                "lead_time_std_days": srng.randint(1, 8),
                "on_time_rate": round(srng.uniform(0.78, 0.99), 3),
                "quality_rate": round(srng.uniform(0.94, 0.999), 3),
                "cost_factor": round(srng.uniform(0.88, 1.18), 3),
                "active_flag": True,
            }
        )
    products = []
    for i in range(config.product_count):
        category = CATEGORIES[i % len(CATEGORIES)]
        cost = round(prng.uniform(4.0, 180.0), 2)
        products.append(
            {
                "product_id": f"PRD-{i + 1:05d}",
                "sku": f"SS-{i + 1:05d}",
                "product_name": f"{category} Item {i + 1:03d}",
                "category": category,
                "subcategory": prng.choice(SUBCATEGORIES[category]),
                "unit_cost": cost,
                "list_price": round(cost * prng.uniform(1.35, 2.35), 2),
                "launch_date": _random_date(
                    prng, config.start_date - timedelta(days=365), config.start_date
                ).isoformat(),
                "active_flag": True,
                "reorder_point": prng.randint(8, 45),
            }
        )
    warehouses = []
    selected_regions = REGIONS[:]
    wrng.shuffle(selected_regions)
    for i in range(config.warehouse_count):
        region = selected_regions[i % len(selected_regions)]
        warehouses.append(
            {
                "warehouse_id": f"WH-{i + 1:03d}",
                "warehouse_name": f"{region} Distribution Center {i + 1}",
                "region": region,
                "country": REGION_COUNTRIES[region],
                "capacity_units": wrng.randint(50_000, 250_000),
            }
        )
    customers = []
    for i in range(config.customer_count):
        region = crng.choices(REGIONS, [0.29, 0.23, 0.19, 0.12, 0.17])[0]
        customers.append(
            {
                "customer_id": f"CUS-{i + 1:07d}",
                "customer_segment": crng.choices(
                    ["consumer", "small_business", "enterprise"], [0.70, 0.23, 0.07]
                )[0],
                "region": region,
                "country": "US",
                "signup_date": _random_date(
                    crng, config.start_date - timedelta(days=730), config.start_date
                ).isoformat(),
            }
        )
    return {
        "products": products,
        "warehouses": warehouses,
        "suppliers": suppliers,
        "customers": customers,
    }


def _date_weights(config: GenerationConfig) -> tuple[list[date], list[float]]:
    dates = []
    weights = []
    day = config.start_date
    while day <= config.end_date:
        q = (day.month - 1) // 3 + 1
        seasonal = {1: 0.88, 2: 0.91, 3: 0.96, 4: 1.02}[q]
        if q == 4:
            seasonal = 1.48
        if q == 3:
            seasonal = 1.08
        # Weekend demand is slightly higher; fixed weights preserve reproducibility.
        weights.append(seasonal * (1.12 if day.weekday() >= 5 else 0.94))
        dates.append(day)
        day += timedelta(days=1)
    return dates, weights


def _product_weights(products: list[dict[str, Any]]) -> list[float]:
    weights = []
    for i, product in enumerate(products):
        rank_weight = 1 / ((i + 1) ** 0.78)
        cat_factor = {
            "Electronics": 1.12,
            "Home": 1.0,
            "Outdoor": 0.94,
            "Office": 0.82,
            "Apparel": 0.91,
        }[product["category"]]
        weights.append(rank_weight * cat_factor)
    return weights


def _category_weights(quarter: int) -> list[float]:
    """Category seasonality: outdoor in Q2, office in Q3, and gift categories in Q4."""
    factors = {
        1: {
            "Electronics": 1.0,
            "Home": 1.0,
            "Outdoor": 0.55,
            "Office": 0.95,
            "Apparel": 1.10,
        },
        2: {
            "Electronics": 1.0,
            "Home": 0.95,
            "Outdoor": 1.8,
            "Office": 0.85,
            "Apparel": 1.0,
        },
        3: {
            "Electronics": 0.95,
            "Home": 1.0,
            "Outdoor": 1.2,
            "Office": 1.65,
            "Apparel": 0.95,
        },
        4: {
            "Electronics": 1.7,
            "Home": 1.45,
            "Outdoor": 0.6,
            "Office": 0.9,
            "Apparel": 1.55,
        },
    }
    return [factors[quarter][category] for category in CATEGORIES]


def _simulate_inventory(
    config: GenerationConfig,
    products: list[dict[str, Any]],
    warehouses: list[dict[str, Any]],
    suppliers: list[dict[str, Any]],
    order_lines_by_day: dict[tuple[date, str, str], list[dict[str, Any]]],
    rng: random.Random,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Simulate daily demand, receipts, reorder triggers, and end-of-day stock."""
    rows: list[dict[str, Any]] = []
    product_by_id = {p["product_id"]: p for p in products}
    suppliers_by_region = _group(suppliers, "region")
    supplier_for = {}
    for wi, wh in enumerate(warehouses):
        region_suppliers = suppliers_by_region.get(wh["region"]) or suppliers
        for pi, product in enumerate(products):
            supplier_for[(wh["warehouse_id"], product["product_id"])] = (
                region_suppliers[(pi + wi) % len(region_suppliers)]
            )
    balance = {
        (wh["warehouse_id"], p["product_id"]): max(
            p["reorder_point"] * 2, rng.randint(25, 60)
        )
        for wh in warehouses
        for p in products
    }
    cumulative = Counter()
    in_transit: Counter[tuple[str, str]] = Counter()
    inbound: defaultdict[
        tuple[date, str, str], list[tuple[int, int, dict[str, Any]]]
    ] = defaultdict(list)
    open_po: set[tuple[str, str]] = set()
    shipment_allocations: dict[str, int] = {}
    po_num = 0
    inventory_path = config.output_dir / "inventory_snapshots.csv"
    with inventory_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TABLE_COLUMNS["inventory_snapshots"])
        writer.writeheader()
        day = config.start_date
        while day <= config.end_date:
            for wh in warehouses:
                for product in products:
                    key = (wh["warehouse_id"], product["product_id"])
                    arrived = inbound.pop((day, *key), [])
                    if arrived:
                        arriving_units = sum(quantity for quantity, _, _ in arrived)
                        transit_units = sum(quantity for _, quantity, _ in arrived)
                        balance[key] += arriving_units
                        in_transit[key] -= transit_units
                        open_po.discard(key)
                    fulfilled_today = 0
                    for order_line in order_lines_by_day.get((day, *key), []):
                        fulfilled = min(int(order_line["quantity"]), balance[key])
                        balance[key] -= fulfilled
                        fulfilled_today += fulfilled
                        shipment_allocations[order_line["order_line_id"]] = fulfilled
                    cumulative[key] += fulfilled_today
                    if balance[key] <= product["reorder_point"] and key not in open_po:
                        supplier = supplier_for[key]
                        lead = supplier["lead_time_days"]
                        order_date = day
                        expected = order_date + timedelta(days=lead)
                        actual_lead = max(
                            1, round(rng.gauss(lead, supplier["lead_time_std_days"]))
                        )
                        if rng.random() > supplier["on_time_rate"]:
                            actual_lead += rng.randint(
                                1, max(1, supplier["lead_time_std_days"] * 2)
                            )
                        scheduled_receipt_date = (
                            order_date + timedelta(days=actual_lead)
                            if rng.random() < 0.99
                            else None
                        )
                        # Target roughly six weeks of observed demand.
                        elapsed = max(1, (day - config.start_date).days + 1)
                        daily_rate = cumulative[key] / elapsed
                        target = max(
                            product["reorder_point"] * 3, round(daily_rate * 42)
                        )
                        ordered = max(
                            product["reorder_point"],
                            target - balance[key] - in_transit[key],
                        )
                        received_qty = (
                            ordered
                            if rng.random() < supplier["quality_rate"]
                            else max(1, round(ordered * rng.uniform(0.80, 0.96)))
                        )
                        po_num += 1
                        received_by_cutoff = bool(
                            scheduled_receipt_date
                            and scheduled_receipt_date <= config.end_date
                        )
                        po = {
                            "purchase_order_id": f"PO-{po_num:08d}",
                            "po_line_id": f"PO-{po_num:08d}-L01",
                            "supplier_id": supplier["supplier_id"],
                            "warehouse_id": key[0],
                            "product_id": key[1],
                            "order_date": order_date.isoformat(),
                            "expected_delivery_date": expected.isoformat(),
                            "received_date": scheduled_receipt_date.isoformat()
                            if received_by_cutoff
                            else "",
                            "ordered_quantity": ordered,
                            "received_quantity": received_qty
                            if received_by_cutoff
                            else 0,
                            "unit_cost": round(
                                product["unit_cost"] * supplier["cost_factor"], 2
                            ),
                            "status": (
                                "received"
                                if received_by_cutoff
                                else "in_transit"
                                if scheduled_receipt_date
                                else "open"
                            ),
                        }
                        rows.append(po)
                        if scheduled_receipt_date:
                            inbound[(scheduled_receipt_date, *key)].append(
                                (received_qty, ordered, po)
                            )
                            in_transit[key] += ordered
                        open_po.add(key)
                    if (
                        day - config.start_date
                    ).days % config.inventory_interval_days == 0:
                        writer.writerow(
                            {
                                "snapshot_date": day.isoformat(),
                                "warehouse_id": key[0],
                                "product_id": key[1],
                                "on_hand_units": balance[key],
                                "reserved_units": 0,
                                "available_units": balance[key],
                                "in_transit_units": in_transit[key],
                                "inventory_value": round(
                                    balance[key] * product_by_id[key[1]]["unit_cost"], 2
                                ),
                            }
                        )
            day += timedelta(days=1)
    return rows, shipment_allocations


def _make_fulfillment(
    config: GenerationConfig,
    order_lines: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]],
    shipment_allocations: dict[str, int],
    ship_rng: random.Random,
    return_rng: random.Random,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    shipments = []
    returns = []
    for i, (order, product, warehouse) in enumerate(order_lines):
        planned_status = order["status"]
        if planned_status == "cancelled":
            continue
        shipped_quantity = shipment_allocations.get(order["order_line_id"], 0)
        if shipped_quantity <= 0:
            order["status"] = "backordered"
            continue
        order_date = date.fromisoformat(order["order_date"])
        ship_day = order_date
        delivered = planned_status == "delivered" and ship_rng.random() < 0.985
        delivery = (
            ship_day + timedelta(days=ship_rng.randint(1, 7)) if delivered else None
        )
        if shipped_quantity < int(order["quantity"]):
            order["status"] = "partially_fulfilled"
        else:
            order["status"] = "delivered" if delivered else "processing"
        shipments.append(
            {
                "shipment_id": f"SHP-{i + 1:08d}",
                "order_line_id": order["order_line_id"],
                "warehouse_id": warehouse["warehouse_id"],
                "ship_date": ship_day.isoformat(),
                "delivery_date": delivery.isoformat() if delivery else "",
                "shipped_quantity": shipped_quantity,
                "shipment_status": "delivered" if delivered else "in_transit",
                "carrier": ship_rng.choice(
                    [
                        "Northstar Parcel",
                        "Continental Freight",
                        "RapidShip",
                        "Pioneer Logistics",
                    ]
                ),
            }
        )
        if delivered and return_rng.random() < 0.065:
            ret_date = delivery + timedelta(days=return_rng.randint(2, 30))
            ret_qty = return_rng.randint(1, shipped_quantity)
            returns.append(
                {
                    "return_id": f"RET-{len(returns) + 1:08d}",
                    "order_line_id": order["order_line_id"],
                    "return_date": ret_date.isoformat(),
                    "quantity": ret_qty,
                    "reason": return_rng.choice(
                        [
                            "damaged",
                            "wrong_item",
                            "not_as_described",
                            "changed_mind",
                            "late_delivery",
                        ]
                    ),
                    "disposition": return_rng.choice(
                        ["quarantine", "refurbish", "dispose"]
                    ),
                    "refund_amount": round(
                        ret_qty * order["unit_price"] * (1 - order["discount_pct"]), 2
                    ),
                }
            )
    return shipments, returns


def _write_csv(path: Path, table: str, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TABLE_COLUMNS[table])
        writer.writeheader()
        writer.writerows(rows)


def _count_csv(path: Path) -> int:
    with path.open(encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[row[key]].append(row)
    return out


def _random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, max(0, (end - start).days)))
