# Monthly demand forecasting

## Purpose and source

The forecasting pipeline estimates gross requested units for each product and warehouse over the next three calendar months. Its output is intended as a demand input for later planning analysis; it does not make replenishment decisions. The grain is stable `product_id` × CORE `warehouse_key` × forecast month. The current `product_key` is carried for dimensional joins, while historical demand is grouped by business `product_id` so observed product versions do not split a series.

The source is `SUPPLY_CHAIN_DEV.MARTS.SALES_ORDER_LINE`, joined to CORE date and product dimensions. The target is the monthly sum of eligible order-line `quantity`, across delivered, processing, backordered, partially fulfilled, and cancelled statuses. Current order status is not used as a predictor or historical filter because the source has no status-change timestamps. This target measures recorded order intent, not realized sales or unconstrained latent demand. Returns are reported separately and are not subtracted. CORE excludes quarantined and otherwise ineligible order rows; CLEAN and WARNING rows remain eligible. The source query permits only these two quality states. Stockouts may suppress observed orders, and the pipeline does not estimate that lost demand.

A read-only DEV inspection on 2026-09-25 found 4,950 eligible mart order lines from January through December 2024. They formed 120 product × warehouse series, with 6–12 active months per series (median 11). The generated source contained 5,000 order lines; 50 invalid-discount rows were excluded before the mart. The mart inspection found no WARNING order lines. This one-year history supports a cautious monthly model; it cannot establish annual seasonality or long-run trend stability.

## Time index and model policy

The CLI requires an explicit coverage start and observed-through date. Coverage must start on the first day of a month and end on the last day of a complete month. Within that declared window, each observed product–warehouse pair begins at its first recorded order month. Later absent months are zero demand; months before the first recorded order remain unknown. Demand is never forward-filled. The Airflow task obtains the declared period from the validated source report, and the forecast read remains limited to that window. Airflow requires that period to end in the previous calendar month, so all three forecast months are current or future. The fixed 2024 DEV sample therefore cannot be published as a current operational forecast in 2026. An operator using the CLI directly is responsible for verifying the stated source coverage.

Each run uses at most the latest 12 complete monthly observations per series. The baseline repeats the mean of the latest three months. The candidate is a fixed-parameter, non-seasonal damped Holt level-and-trend forecast. Both consume only past demand; the baseline's three-month average is recomputed from observations available at each validation origin. No future inventory, supplier, return, fulfillment, holiday, or product-category values enter training. Category and warehouse remain descriptive context, not fitted predictors. No annual seasonal component is fitted from the single annual cycle.

Chronological validation reserves the final three observed months for a test holdout. Three expanding-window, one-month validation origins immediately precede that holdout; each has at least six prior months. The damped-trend candidate is considered only with 12 complete indexed months and at least four nonzero months. It must improve mean validation MAE across the paired eligible population before any series can select it. An eligible series then uses it only when its own candidate MAE is strictly lower than its baseline MAE. Otherwise it uses the baseline. Newly observed, sparse, and nearly constant series therefore have an explicit safe path. Products with no eligible order history are outside the observed series universe; no demand is fabricated for them.

Evaluation records include MAE, RMSE, and WAPE for the selected model and both candidates where evaluable, plus final test metrics. WAPE is undefined when total actual demand is zero and is stored as null. Selection uses MAE so an individual low-volume series does not rely on an unstable percentage error. Population comparisons should be made from all evaluated series, not a favorable example.

### DEV evaluation on the 2024 mart

The read-only run evaluated all 120 observed series. Both models could be compared on 101 series with a full 12-month index. On those paired series, mean validation MAE was **5.11 units** for the trailing three-month mean and **5.99 units** for damped Holt. The population gate therefore selected the baseline for all 120 series. This is a measured fallback decision, not evidence that the trend model improved accuracy.

For the untouched October–December 2024 holdout, the selected baseline across all 120 series had **MAE 7.09 units**, **RMSE 10.17 units**, and **WAPE 61.05%** against 4,180 actual units. These errors are substantial; the one-year synthetic history does not support a claim of reliable production accuracy. The historical dry run produced 360 nonnegative January–March 2025 forecasts with no series failures or unavailable empirical bounds. These forecast dates are now past; the DEV write validation demonstrates pipeline behavior rather than current demand planning. Results may change when source history or model code changes.

Forecast bounds are empirical error bands, not statistical confidence intervals. They use validation errors at the same forecast lead for the selected model where enough are available and pooled same-lead validation errors for series with little history. The method and contributing sample count are recorded with each forecast. When no lead-specific calibration sample is available, bounds are null and the method is marked unavailable. Lower bounds are clipped at zero; predictions and upper bounds cannot be negative. Bounds are deliberately descriptive with only one year of history.

An approved DEV persistence validation on 2026-09-25 created the three tables in the existing `SUPPLY_CHAIN_DEV.FORECASTING` schema and wrote run `c75ee72b-e162-5866-9d3a-9c88699e15a8`. The run stored 360 forecast rows, 120 per-series evaluation rows, and one `SUCCEEDED` metadata row: 120 series attempted and successful, 120 baseline fallbacks, and zero failed. All 360 forecasts had nonnegative predictions and ordered, nonnull bounds; 101 series had paired baseline/candidate validation metrics and all 120 had test metrics. Repeating the same run ID left the counts unchanged at 360/120/1. These January–March 2025 forecasts are historical validation output, not current operational forecasts.

## Objects and run behavior

The DEV-only SQL definition is [the forecasting schema script](../sql/forecasting/001_forecasting_tables.sql). It defines the schema when authorized and these tables:

| Object | Grain and purpose |
| --- | --- |
| `SUPPLY_CHAIN_DEV.FORECASTING.CURRENT_FORECASTS` | Current product × warehouse × future month forecasts, selected model, fallback state, bounds, training window, and run lineage. |
| `SUPPLY_CHAIN_DEV.FORECASTING.MODEL_EVALUATION` | Per-run, per-series model comparison and chronological test metrics. |
| `SUPPLY_CHAIN_DEV.FORECASTING.RUN_METADATA` | Run status, timestamps, period, horizon, series/record counts, version and failure context. |

Runtime code does not create warehouse objects. It checks the configured and active Snowflake context before reading or writing. Persistence replaces the current forecast set and the same run's evaluation/metadata inside one transaction. A rerun of the same run ID does not append duplicate current forecasts or evaluation rows. Snowflake standard-table primary keys document the intended grain; the transactional replacement supplies the operational idempotency. Failed persistence rolls back so the prior current forecasts remain available. Airflow keeps forecasting after the successful warehouse dbt refresh and uses the shared mutation pool; a failed upstream layer prevents the forecast task from starting.

## Running and validation

With DEV credentials configured as in [the staging runbook](staging.md), read and evaluate without writing:

```powershell
python -m supplysight.forecasting --coverage-start 2024-01-01 --observed-through 2024-12-31 --horizon 3 --dry-run
```

After approved creation of the DEV forecasting objects, omit `--dry-run` for a persisted historical validation run. An optional `--run-id` identifies an explicit rerun; without it the pipeline derives an ID from the sorted input and configuration. The same source values and configuration produce the same demand predictions. Run timestamps can differ. The existing manual Airflow ingestion DAG triggers the warehouse DAG, which runs forecasting only after its MARTS tests and reconciliations pass. Its freshness guard requires a validated source period ending in the month immediately before execution. Triggering either DAG can reach Snowflake writes and remains subject to the project approval policy.

Run Python tests and Ruff locally, dbt parse, the existing `sales_order_line` dbt tests, and the Airflow DAG import check before a live forecast. For an approved DEV validation, inspect the three tables for the run ID, reconcile forecast rows to successful series × horizon, verify all predicted values and bounds are ordered and nonnegative, check baseline/candidate metrics and fallback counts, and rerun the same run ID to confirm replacement behavior. Do not point this workflow at another Snowflake database or warehouse.

## Limits

Only one year of current-state source history is available. Quarantined order lines are absent from the target, status history is unavailable, and stockout-censored demand cannot be recovered. Product attributes before the first observed SCD version are fallback context rather than verified historical attributes. Short backtests and empirical intervals may be unstable, particularly for low-volume series. Reassess the grain, candidate model, and interval calibration when multiple years of reliably complete history become available.
