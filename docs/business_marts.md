# Business marts

## Scope and build

dbt builds business-ready tables in `SUPPLY_CHAIN_DEV.MARTS` using `SUPPLY_CHAIN_DEV_WH`. The five domains are sales, inventory, supplier, logistics, and executive reporting. Their measures read conformed CORE facts and dimensions. The exception audit view reads the existing INTERMEDIATE exception register because excluded records do not appear in CORE facts. No mart reads RAW or staging. Full table materialization is appropriate for the current DEV volume; `mart_quality_exceptions` remains a view.

Run `tag:marts` after refreshing CORE. The dbt profile and schema macro pin the database and warehouse to DEV. `dbt run` requires approval under the project command policy. The mart build does not refresh product snapshots or change earlier layers.

| Model | Grain and key | Purpose |
| --- | --- | --- |
| `sales_order_line` | One eligible CORE order line; `order_line_id` | Recognized revenue, cost, profit, order activity, and linked return context. |
| `sales_monthly` | Order month, customer, product version, warehouse | Sales and order-status trends. |
| `return_monthly` | Return month, customer, product version, warehouse | Authoritative eligible return and refund trends. |
| `inventory_detail` | One CORE inventory snapshot; `inventory_snapshot_key` | Traceable balances and stockout/excess flags. |
| `inventory_monthly` | Month, product business key, warehouse business key | Observed balances, availability, stockouts, trailing demand, turnover, supply, excess, dead stock. |
| `supplier_detail` | One CORE purchase order line; `po_line_id` | Spend, due-cohort service flags, and receipt measures. |
| `supplier_monthly` | Month, supplier | Spend by order month and service by expected-delivery month. |
| `shipment_detail` | One CORE shipment; `shipment_id` | Shipment, delivery, timeliness, and inherited order quality. |
| `logistics_monthly` | Ship month, carrier, warehouse | Shipment and delivery trends. |
| `mart_quality_exceptions` | One excluded fact-grain source key and entity | Quality status, exclusion reason, and ingestion lineage. |
| `mart_quality_exceptions_monthly` | Ingestion month, source entity, exclusion reason | Exception counts; its time axis is ingestion, not event time. |
| `mart_executive_monthly` | Calendar month | Preaggregated domain measures and management rates. |

Detail models retain CORE fact keys, business identifiers, dimension keys, quality status/issues, product-history fallback, and available batch/source lineage. Monthly models retain the relevant grouping keys, additive numerators and denominators, and warning/fallback counts. `inventory_monthly` also retains the selected month-end CORE inventory snapshot key. The executive model joins domain aggregates after each has been reduced to one row per month, preventing cross-fact multiplication.

## KPI contract

Rates are calculated from summed numerators and denominators, never by averaging row rates. A zero or absent denominator yields `NULL`; a missing cohort count is zero in the executive output. Monetary values are source-currency amounts; the contracts provide no currency conversion.

| KPI | Definition and cohort |
| --- | --- |
| Booked order amount | Sum CORE `net_order_amount` for all eligible order lines, including open and cancelled statuses; this is order activity, not recognized revenue. |
| Revenue | Sum CORE `net_order_amount` only where order status is `delivered`. Returns are reported separately and are not netted without a refund recognition policy. |
| COGS | Delivered order quantity × the linked CORE product-version `unit_cost`, summed across delivered lines. |
| Gross profit / margin | Revenue − COGS; gross profit ÷ revenue. |
| Returns / refunds | Count, returned units, and `refund_amount` from all eligible CORE returns by return month. `linked_*` columns on sales order lines are order-date context and do not replace return-month totals. |
| Inventory value | Sum the last observed `inventory_value` for each product and warehouse in a month. Average inventory value sums per-group averages of observed snapshots; it is the turnover denominator. Snapshot values are balances and must not be summed across dates. |
| Supplier spend | Sum CORE PO `ordered_cost` by order month, regardless of receipt status. |
| Supplier OTIF | For PO lines with expected delivery date on or before the observed data cutoff, 1 when receipt date is on/before expected date **and** received quantity meets ordered quantity; passing lines ÷ due lines. Service groups by expected-delivery month. |
| Supplier fill rate | Sum `least(received_quantity, ordered_quantity)` ÷ sum ordered quantity for the same due cohort. Over-receipts cannot raise the rate above 100%. |
| Lead time / variability | Receipt date − order date in calendar days for received PO lines that also have an expected-delivery date. Average is sum days ÷ observed lines; variability is sample standard deviation and is `NULL` with fewer than two observations. These measures group by expected-delivery month. |
| Late purchase orders | Due PO lines with expected date before the observed cutoff whose receipt was after expected date, has not arrived, or was short. This is a line count, grouped by expected-delivery month. |
| Stockout rate | Observed inventory snapshots with available units ≤ 0 ÷ observed eligible snapshots. A sparse snapshot cadence measures observed stockouts, not all calendar days. |
| Excess inventory | At the last observed monthly snapshot, `greatest(available_units − reorder_point, 0)`; value uses the snapshot's per-on-hand-unit valuation. It is a descriptive threshold measure, not a recommendation. |
| Dead stock | Positive available units at the last observed monthly snapshot and zero delivered order units for that product and warehouse in the trailing 90 calendar days; units and value are reported. |
| Inventory turnover | Executive monthly delivered-order COGS ÷ average observed inventory value for the same month. Inventory domain also exposes trailing-90-day delivered COGS ÷ monthly average inventory value. Neither is annualized. |
| Days of supply | Executive average available units ÷ (delivered units in month ÷ calendar days in month). Inventory domain also exposes average available units ÷ (trailing-90-day delivered units ÷ 90). Zero demand yields `NULL`, not infinite days. |
| Logistics | Shipments and shipped units by ship month; completed count requires a delivery date. Timeliness requires both delivery and promised dates. On-time count ÷ timeliness-eligible count; transit days are delivery date − ship date. |

The supplier as-of cutoff is the maximum CORE purchase-order order date, rather than the date a mart is rebuilt. This keeps historical DEV results reproducible. The supplier monthly row deliberately contains two date cohorts: spend and PO activity use order month, while OTIF, fill, and late counts use expected-delivery month. Null expected dates are outside the service cohort. Executive months use each domain's documented calendar basis, and exception counts use ingestion month.

## Quality and product history

CORE already excludes fact rows with known invalid measures or event attributes. `WARNING` rows remain eligible in all measures; marts retain their status and count them separately. `QUARANTINED` and otherwise ineligible records enter `mart_quality_exceptions` with source entity/key and reason, and do not contribute to business measure numerators or denominators. The exception register reconciles to the existing INTERMEDIATE view. An eligible shipment or return linked to a quarantined order remains included, while its `upstream_order_quality_status` is exposed and counted. Shipment timeliness is unavailable when an eligible order's promised date cannot be resolved; such shipments remain in shipment counts and leave the timeliness denominator.

The product dimension is warehouse-observed SCD Type 2. `product_history_fallback` identifies events before the first observed product version; detail marts retain the flag, and monthly/executive marts publish affected-row counts. All current 2024 facts use this fallback. Cost, margin, excess, and dead-stock valuations derived from those facts use the earliest observed product attributes and cannot be represented as verified 2024 product attributes. RAW retains current rows only, and date-only source events cannot resolve within-day product changes.

## Validation and downstream ownership

dbt owns cohort eligibility, stable event flags, monetary formulas, additive components, and published ratios. Power BI can later calculate slicer-sensitive presentation measures, period comparisons, running totals, and display formatting from these components; it should not redefine eligibility. Monthly revenue, units, inventory, and supplier series are analytics-ready inputs for later forecasting, but no forecast or recommendation is produced in this phase.

Run parse and compile with the DEV profile before the approved mart build, then run `dbt test --select tag:marts` and direct reconciliation queries. Schema tests cover required keys, uniqueness, relationships, and quality/status values. Singular tests compare detail and monthly business measures to CORE, bound service and inventory ratios, and check executive rollups, exception counts, and fallback coverage. Inspect DEV object names before and after the build to confirm that only `SUPPLY_CHAIN_DEV.MARTS` changed.
