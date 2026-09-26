import csv
from collections import Counter
from dataclasses import replace
from datetime import date
from pathlib import Path

from supplysight.data_generation.config import GenerationConfig, config_for_profile
from supplysight.data_generation.generate import (
    _category_weights,
    _date_weights,
    generate_dataset,
)
from supplysight.data_generation.validation import validate_dataset


def _config(
    path: Path, *, lines: int = 300, defect_rate: float = 0.01
) -> GenerationConfig:
    return GenerationConfig(
        profile="test",
        seed=417,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
        output_dir=path,
        product_count=10,
        warehouse_count=3,
        supplier_count=10,
        customer_count=80,
        order_line_count=lines,
        defect_rate=defect_rate,
    )


def _read(path: Path, filename: str) -> list[dict[str, str]]:
    with (path / filename).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_generation_is_byte_reproducible(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_dataset(_config(first, lines=200), clean=True)
    generate_dataset(_config(second, lines=200), clean=True)
    for filename in sorted(p.name for p in first.iterdir()):
        assert (first / filename).read_bytes() == (second / filename).read_bytes()


def test_relationships_chronology_and_inventory_are_valid(tmp_path: Path) -> None:
    report = generate_dataset(_config(tmp_path, lines=450), clean=True)
    assert report["validation"]["valid"] is True
    orders = _read(tmp_path, "orders.csv")
    shipments = _read(tmp_path, "shipments.csv")
    returns = _read(tmp_path, "returns.csv")
    products = {r["product_id"] for r in _read(tmp_path, "products.csv")}
    customers = {r["customer_id"] for r in _read(tmp_path, "customers.csv")}
    line_ids = {r["order_line_id"] for r in orders}
    assert len(line_ids) == len(orders)
    assert len({r["order_id"] for r in orders}) < len(orders)
    assert all(
        r["product_id"] in products and r["customer_id"] in customers for r in orders
    )
    order_by_line = {r["order_line_id"]: r for r in orders}
    for shipment in shipments:
        order = order_by_line[shipment["order_line_id"]]
        assert shipment["ship_date"] >= order["order_date"]
        if shipment["delivery_date"]:
            assert shipment["delivery_date"] >= shipment["ship_date"]
    shipment_by_line = {r["order_line_id"]: r for r in shipments}
    for ret in returns:
        assert ret["order_line_id"] in line_ids
        assert ret["disposition"] in {"quarantine", "refurbish", "dispose"}
        ship = shipment_by_line[ret["order_line_id"]]
        if ship["delivery_date"]:
            assert ret["return_date"] >= ship["delivery_date"]
    snapshots = _read(tmp_path, "inventory_snapshots.csv")
    assert snapshots
    assert all(int(r["on_hand_units"]) >= 0 for r in snapshots)
    assert all(
        r["reserved_units"] == "0" and r["available_units"] == r["on_hand_units"]
        for r in snapshots
    )


def test_defect_switch_is_deterministic_and_clean_mode_disables_it(
    tmp_path: Path,
) -> None:
    dirty = tmp_path / "dirty"
    dirty_again = tmp_path / "dirty_again"
    clean = tmp_path / "clean"
    dirty_report = generate_dataset(_config(dirty, lines=400, defect_rate=1.0))
    generate_dataset(_config(dirty_again, lines=400, defect_rate=1.0))
    clean_report = generate_dataset(
        _config(clean, lines=400, defect_rate=1.0), clean=True
    )
    assert all(v > 0 for v in dirty_report["defect_counts"].values())
    assert not any(clean_report["defect_counts"].values())
    assert all(dirty_report["validation"]["injected_defect_counts_match"].values())
    assert all(clean_report["validation"]["injected_defect_counts_match"].values())
    for filename in sorted(p.name for p in dirty.iterdir()):
        assert (dirty / filename).read_bytes() == (dirty_again / filename).read_bytes()
    assert any(not row["subcategory"] for row in _read(dirty, "products.csv"))
    assert not any(not row["subcategory"] for row in _read(clean, "products.csv"))
    assert any(float(row["discount_pct"]) > 1 for row in _read(dirty, "orders.csv"))
    assert not any(float(row["discount_pct"]) > 1 for row in _read(clean, "orders.csv"))
    assert (
        dirty_report["validation"]["data_quality_findings"][
            "inventory_balance_mismatch"
        ]
        > 0
    )
    assert (
        clean_report["validation"]["data_quality_findings"][
            "inventory_balance_mismatch"
        ]
        == 0
    )


def test_profiles_scale_and_daily_snapshot_defaults() -> None:
    dev = config_for_profile("dev")
    portfolio = config_for_profile("portfolio")
    assert (
        dev.product_count,
        dev.warehouse_count,
        dev.supplier_count,
        dev.customer_count,
        dev.order_line_count,
    ) == (40, 3, 12, 500, 5_000)
    assert (
        portfolio.product_count,
        portfolio.warehouse_count,
        portfolio.supplier_count,
        portfolio.customer_count,
        portfolio.order_line_count,
    ) == (250, 8, 40, 10_000, 250_000)
    assert dev.inventory_interval_days == 1
    assert portfolio.inventory_interval_days == 7


def test_seasonality_and_regional_demand_patterns(tmp_path: Path) -> None:
    seasonal_config = _config(tmp_path, lines=8_000)
    seasonal_config = GenerationConfig(
        **{
            **seasonal_config.__dict__,
            "end_date": date(2024, 12, 31),
        }
    )
    dates, weights = _date_weights(seasonal_config)
    quarter_weights = {
        q: sum(
            weight
            for day, weight in zip(dates, weights)
            if (day.month - 1) // 3 + 1 == q
        )
        for q in (1, 2, 3, 4)
    }
    assert quarter_weights[4] > quarter_weights[1]
    categories = ["Electronics", "Home", "Outdoor", "Office", "Apparel"]
    q2 = dict(zip(categories, _category_weights(2)))
    q4 = dict(zip(categories, _category_weights(4)))
    assert q2["Outdoor"] > q4["Outdoor"]
    assert q4["Electronics"] > q2["Electronics"]

    generate_dataset(seasonal_config, clean=True)
    orders = _read(tmp_path, "orders.csv")
    customers = {r["customer_id"]: r for r in _read(tmp_path, "customers.csv")}
    products = {r["product_id"]: r for r in _read(tmp_path, "products.csv")}
    regional_demand = Counter()
    quarter_category_demand = Counter()
    quarterly_lines = Counter()
    for order in orders:
        region = customers[order["customer_id"]]["region"]
        quantity = int(order["quantity"])
        regional_demand[region] += quantity
        quarter = (date.fromisoformat(order["order_date"]).month - 1) // 3 + 1
        category = products[order["product_id"]]["category"]
        quarter_category_demand[(quarter, category)] += quantity
        quarterly_lines[quarter] += 1
    assert regional_demand["Northeast"] > regional_demand["Southwest"]
    assert quarterly_lines[4] > quarterly_lines[1]
    assert (
        quarter_category_demand[(2, "Outdoor")]
        > quarter_category_demand[(4, "Outdoor")]
    )
    assert (
        quarter_category_demand[(4, "Electronics")]
        > quarter_category_demand[(2, "Electronics")]
    )


def test_stockouts_limit_shipments_and_future_receipts_remain_in_transit(
    tmp_path: Path,
) -> None:
    config = replace(
        _config(tmp_path, lines=2_000),
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 7),
    )
    report = generate_dataset(config, clean=True)
    assert report["validation"]["valid"]
    orders = _read(tmp_path, "orders.csv")
    shipments = _read(tmp_path, "shipments.csv")
    shipped = Counter()
    for row in shipments:
        shipped[row["order_line_id"]] += int(row["shipped_quantity"])
    assert any(row["status"] == "backordered" for row in orders)
    assert all(shipped[row["order_line_id"]] <= int(row["quantity"]) for row in orders)
    assert any(
        row["status"] == "in_transit" for row in _read(tmp_path, "purchase_orders.csv")
    )
    assert all(
        row["received_date"] == "" and row["received_quantity"] == "0"
        for row in _read(tmp_path, "purchase_orders.csv")
        if row["status"] == "in_transit"
    )


def test_validation_rejects_duplicate_inventory_snapshot(tmp_path: Path) -> None:
    config = _config(tmp_path, lines=100)
    generate_dataset(config, clean=True)
    path = tmp_path / "inventory_snapshots.csv"
    with path.open(newline="", encoding="utf-8") as stream:
        first = next(csv.DictReader(stream))
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(first))
        writer.writerow(first)
    validation = validate_dataset(tmp_path, as_of_date=config.end_date)
    assert not validation["valid"]
    assert any(
        "duplicate date, warehouse, product key" in error
        for error in validation["errors"]
    )
