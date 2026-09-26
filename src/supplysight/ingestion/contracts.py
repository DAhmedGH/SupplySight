"""Source CSV contracts and source-to-RAW identifiers."""

from __future__ import annotations

from supplysight.data_generation.schema import TABLE_COLUMNS

SOURCE_SYSTEM = "SUPPLY_SIGHT_SYNTHETIC"
ENTITY_ORDER = tuple(TABLE_COLUMNS)
BUSINESS_KEYS = {
    "products": ("product_id",),
    "warehouses": ("warehouse_id",),
    "suppliers": ("supplier_id",),
    "customers": ("customer_id",),
    "orders": ("order_line_id",),
    "inventory_snapshots": ("snapshot_date", "warehouse_id", "product_id"),
    "purchase_orders": ("po_line_id",),
    "shipments": ("shipment_id",),
    "returns": ("return_id",),
}
FOREIGN_KEYS = {
    "orders": {
        "customer_id": "customers",
        "product_id": "products",
        "warehouse_id": "warehouses",
    },
    "inventory_snapshots": {"product_id": "products", "warehouse_id": "warehouses"},
    "purchase_orders": {
        "supplier_id": "suppliers",
        "warehouse_id": "warehouses",
        "product_id": "products",
    },
    "shipments": {"order_line_id": "orders", "warehouse_id": "warehouses"},
    "returns": {"order_line_id": "orders"},
}

METADATA_COLUMNS = (
    "_INGESTED_AT",
    "_SOURCE_FILE",
    "_BATCH_ID",
    "_SOURCE_SYSTEM",
    "_ROW_HASH",
    "_QUALITY_STATUS",
    "_QUALITY_ISSUES",
)
