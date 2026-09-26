# Synthetic operational data contracts

## Scope and reproducibility

Phase 2 creates local synthetic source data for the SupplySight supply-chain scenario. The generator writes CSV files only. It does not connect to Snowflake or load data, run dbt, build forecasts, or update Power BI assets. Generated data belongs under `data/generated/`, which is excluded from Git.

Defaults are seed `20240924`, start date `2024-01-01`, end date `2024-12-31` (inclusive), defect rate `0.01`, and output directory `data/generated`. For identical profile, seed, date range, defect rate, inventory interval, and clean setting, generation produces the same logical records and file contents. Output paths do not affect record values. The development profile is intended for quick iteration; portfolio is intended for larger local analysis. Profile dimensions and default inventory cadence are:

| Profile | Products | Warehouses | Suppliers | Customers | Order lines | Inventory snapshots |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `dev` | 40 | 3 | 12 | 500 | 5,000 | `days × 40 × 3` (43,920 for default 366 days; daily) |
| `portfolio` | 250 | 8 | 40 | 10,000 | 250,000 | One snapshot every 7 days from the start date: 53 × 250 × 8 = 106,000 for default 366 days |

Master and order-line counts are fixed by profile; shipments and returns depend on order statuses and fulfillment outcomes. Inventory rows vary with the inclusive date range and cadence. Inventory grain is one row per snapshot date, warehouse, and product. `--inventory-interval-days` overrides the profile cadence and must be a positive integer.

## File contracts

All files are UTF-8 CSV with a header row. Dates use ISO `YYYY-MM-DD`; timestamps, when present in future contract revisions, must use ISO 8601. Identifiers are stable strings within a generated dataset. Integer quantities are whole units and monetary/rate values are decimal numbers. Boolean flags are serialized consistently as `true` or `false`. Blank optional values represent missing values; they are not the strings `NULL` or `None`.

| File | Grain / primary key | Columns (type; `?` means nullable) |
| --- | --- | --- |
| `products.csv` | One product; `product_id` | `product_id` (string), `sku` (string), `product_name` (string), `category` (string), `subcategory` (string; nullable), `unit_cost` (decimal), `list_price` (decimal), `launch_date` (date), `active_flag` (boolean), `reorder_point` (integer) |
| `warehouses.csv` | One warehouse; `warehouse_id` | `warehouse_id` (string), `warehouse_name` (string), `region` (string), `country` (string), `capacity_units` (integer) |
| `suppliers.csv` | One supplier; `supplier_id` | `supplier_id` (string), `supplier_name` (string), `region` (string), `country` (string), `lead_time_days` (integer), `lead_time_std_days` (decimal), `on_time_rate` (decimal), `quality_rate` (decimal), `cost_factor` (decimal), `active_flag` (boolean) |
| `customers.csv` | One customer; `customer_id` | `customer_id` (string), `customer_segment` (string), `region` (string), `country` (string), `signup_date` (date) |
| `orders.csv` | One order line; `order_line_id` (globally unique) | `order_id` (string), `order_line_id` (string PK), `order_date` (date), `customer_id` (string FK), `product_id` (string FK), `warehouse_id` (string FK), `quantity` (integer), `unit_price` (decimal), `discount_pct` (decimal), `status` (enum: `delivered`, `processing`, `backordered`, `partially_fulfilled`, `cancelled`), `promised_delivery_date` (date) |
| `inventory_snapshots.csv` | One product at one warehouse on one date; (`snapshot_date`, `warehouse_id`, `product_id`) | `snapshot_date` (date), `warehouse_id` (string FK), `product_id` (string FK), `on_hand_units` (integer), `reserved_units` (integer), `available_units` (integer), `in_transit_units` (integer), `inventory_value` (decimal) |
| `purchase_orders.csv` | One purchase order line; `po_line_id` (globally unique) | `purchase_order_id` (string), `po_line_id` (string PK), `supplier_id` (string FK), `warehouse_id` (string FK), `product_id` (string FK), `order_date` (date), `expected_delivery_date` (date), `received_date` (date; nullable until received), `ordered_quantity` (integer), `received_quantity` (integer), `unit_cost` (decimal), `status` (enum: `received`, `in_transit`, `open`; reflects state at the generated period cutoff) |
| `shipments.csv` | One shipment record; `shipment_id` | `shipment_id` (string), `order_line_id` (string FK), `warehouse_id` (string FK), `ship_date` (date), `delivery_date` (date; nullable), `shipped_quantity` (integer), `shipment_status` (string), `carrier` (string) |
| `returns.csv` | One return record; `return_id` | `return_id` (string), `order_line_id` (string FK), `return_date` (date), `quantity` (integer), `reason` (enum: `damaged`, `wrong_item`, `not_as_described`, `changed_mind`, `late_delivery`), `disposition` (enum: `quarantine`, `refurbish`, `dispose`), `refund_amount` (decimal) |

The column list is the serialized contract. A new column, changed type, changed grain, or renamed field is a contract change and must be reflected in this document and generator tests.

## Relationships

`orders` references one customer, product, and warehouse per order line. Order lines are identified by the compound order and line identifiers; shipment and return rows link to an order line by `order_line_id`. `inventory_snapshots` references a product and warehouse. `purchase_orders` references a supplier, destination warehouse, and product. Supplier receipts and customer fulfillment are separate processes: a purchase order does not directly reference an order line.

`order_line_id` is globally unique so shipments and returns can reference an order line with that single column. `po_line_id` is globally unique within purchase order lines. The generator creates master entities first (products, warehouses, suppliers, customers), then orders and their fulfillment events and the purchase/inventory history. Inventory snapshots simulate sales demand, replenishment triggers, inbound receipts, on-hand stock, and in-transit quantities. Identifier columns used as foreign keys always resolve to parent keys, including when intentional quality defects are enabled.

Returns are generated only for delivered shipments. Returned units are assigned to `quarantine`, `refurbish`, or `dispose`; none of these dispositions adds units back to sellable inventory. Return events and refunds remain in `returns.csv` and do not alter inventory snapshots.

## Business patterns

The generated series should provide interpretable operational behavior rather than independent random rows:

- Order dates use month/quarter and weekday weights, including a Q4 lift. Outdoor demand peaks in Q2, office in Q3, and electronics, home, and apparel in Q4.
- Customer and warehouse regions influence order assignment; regional demand uses distinct weights.
- Product selection follows a long-tail rank distribution, with category factors affecting demand.
- Supplier on-time rates, lead-time averages/variance, quality rates, and cost factors vary by supplier and drive modeled receipt timing, short receipts, and purchase costs. Natural supplier delays are counted under `validation.business_patterns.supplier_late_receipts` in the report.
- Inventory simulates order demand, reorder points, purchase orders, receipts, and in-transit stock. Demand can reduce balances to zero and trigger replenishment.

The exact numeric multipliers and stochastic parameters are generator configuration details. The seed makes their resulting records repeatable.

## Data-quality issue injection

`--defect-rate` defaults to `0.01`. Defect sampling is deterministic for a fixed seed and rate. The generator writes `generation_report.json` with injected counts and independently measured findings. `--clean` disables intentional injection; naturally late receipts caused by supplier lead-time behavior can still occur. The five deliberate classes are:

- Blank product `subcategory` values (`missing_optional_attribute`).
- Blank `delivery_date` on a shipment marked delivered (`missing_delivery_timestamp`).
- Uppercase customer `region` values (`nonstandard_region_case`).
- `discount_pct` set to `1.20`, outside the normal 0–1 range (`out_of_range_discount`).
- Inventory `available_units` increased by 3 relative to on-hand less reserved (`inventory_balance_mismatch`).

Defects do not alter primary keys or foreign keys. The validation report counts these findings separately from structural and relationship errors. Natural supplier lateness is reported as a business pattern, separate from injected data-quality findings.

## Generation and validation

From the repository root, after installing the project:

```powershell
python -m supplysight.data_generation --profile dev
```

Use `--profile portfolio` for larger datasets. Supported options are `--seed`, `--start-date`, `--end-date`, `--output-dir`, `--defect-rate`, `--inventory-interval-days`, and `--clean`. Dates are inclusive ISO calendar dates. `--clean` disables intentional defects; it does not remove output files. The generator overwrites the nine entity CSVs and `generation_report.json` in the output directory, leaving unrelated files intact.

Generation automatically validates the expected column headers, unique keys, foreign-key references, customer signup chronology, shipment/return chronology and quantities, and purchase order dates and quantities. It reports row counts and deliberate DQ findings in `generation_report.json`; structural failures or unaccounted injected defect classes fail generation. Tests verify deterministic output and profile dimensions. Generated CSVs and the report are excluded from source control; commit generator code, tests, and this contract instead.
