from datetime import date, datetime, timezone

import pytest

from supplysight.forecasting import pipeline
from supplysight.forecasting.dataset import build_series
from supplysight.forecasting.evaluation import metrics, temporal_splits
from supplysight.forecasting.features import lag_features
from supplysight.forecasting.models import fit_damped_holt, moving_average
from supplysight.forecasting.pipeline import run_forecast


def test_dataset_aggregates_months_includes_warning_and_excludes_quarantine():
    rows = [
        {
            "product_id": "p",
            "warehouse_key": "w",
            "demand_date": "2024-01-04",
            "ordered_quantity": 2,
        },
        {
            "product_id": "p",
            "warehouse_key": "w",
            "demand_date": "2024-01-28",
            "ordered_quantity": 3,
            "data_quality_status": "WARNING",
        },
        {
            "product_id": "p",
            "warehouse_key": "w",
            "demand_date": "2024-03-01",
            "ordered_quantity": 8,
            "data_quality_status": "QUARANTINED",
        },
    ]
    series = build_series(
        rows, coverage_start=date(2024, 1, 1), observed_through=date(2024, 3, 1)
    )[0]
    assert series.demand == (5.0, 0.0, 0.0)
    assert series.months[-1] == date(2024, 3, 1)


def test_lags_and_rolling_only_use_previous_demand():
    result = lag_features([1, 2, 30, 40])
    assert result[2]["lag_1"] == 2
    assert result[2]["rolling_mean_3"] == 1.5
    assert result[0]["rolling_mean_3"] == 0


def test_validation_is_before_separate_final_test():
    assert temporal_splits(12) == (6, 7, 8)
    assert temporal_splits(11) == ()


def test_baseline_candidate_metrics_and_nonnegative_predictions():
    assert moving_average([2, 4, 6]) == [4, 4, 4]
    assert all(value >= 0 for value in fit_damped_holt([1, 2, 3]).predict(3))
    score = metrics([1, 3], [2, 1])
    assert score.mae == 1.5
    assert score.wape == 0.75
    assert metrics([0, 0], [0, 0]).wape is None


def test_damped_holt_trend_is_cumulative():
    model = fit_damped_holt([1, 2])
    assert model.predict(2) == pytest.approx(
        [model.level + 0.8 * model.trend, model.level + 1.44 * model.trend]
    )


def test_forecast_run_is_deterministic_falls_back_and_has_clipped_bounds():
    rows = [
        {
            "product_id": "p1",
            "product_key": 41,
            "warehouse_key": "w1",
            "demand_date": f"2024-{month:02d}-01",
            "ordered_quantity": float(month % 3 == 0),
        }
        for month in range(1, 13)
    ]
    timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    one = run_forecast(
        rows,
        generated_at=timestamp,
        observed_through=date(2024, 12, 31),
        coverage_start=date(2024, 1, 1),
    )
    two = run_forecast(
        rows,
        generated_at=timestamp,
        observed_through=date(2024, 12, 31),
        coverage_start=date(2024, 1, 1),
    )
    assert one.run_id == two.run_id
    assert one.forecasts == two.forecasts
    assert (
        one.input_fingerprint
        == run_forecast(
            list(reversed(rows)),
            generated_at=timestamp,
            observed_through=date(2024, 12, 31),
            coverage_start=date(2024, 1, 1),
        ).input_fingerprint
    )
    assert one.series_fallback == 1
    assert all(row.predicted_demand >= 0 for row in one.forecasts)
    assert all(
        row.lower_bound is None
        or row.upper_bound is None
        or (0 <= row.lower_bound <= row.predicted_demand <= row.upper_bound)
        for row in one.forecasts
    )
    assert one.forecasts[0].product_key == "41"
    assert one.forecasts[0].residual_sample_count == 3
    assert (
        one.forecasts[0].uncertainty_method == "validation_abs_error_p90_series_by_lead"
    )
    assert one.model_version == "monthly_demand_v1"


def test_pipeline_requires_explicit_coverage_boundaries():
    with pytest.raises(ValueError, match="must be explicit"):
        run_forecast([], observed_through=date(2024, 1, 1))


def test_population_gate_keeps_baseline_when_candidate_loses_overall(monkeypatch):
    from supplysight.forecasting import pipeline
    from supplysight.forecasting.evaluation import Metrics

    rows = [
        {
            "product_id": product,
            "warehouse_key": "w",
            "demand_date": f"2024-{month:02d}-01",
            "ordered_quantity": month,
        }
        for product in ("a", "b")
        for month in range(1, 13)
    ]

    def score(series, method, _origins, _horizon):
        errors = {
            ("a", "trailing_3_mean"): 5.0,
            ("a", "damped_holt"): 4.0,
            ("b", "trailing_3_mean"): 5.0,
            ("b", "damped_holt"): 8.0,
        }
        value = errors[(series.key.product_id, method)]
        return Metrics(value, value, value), [value] * 3

    monkeypatch.setattr(pipeline, "_score", score)
    run = pipeline.run_forecast(
        rows,
        coverage_start=date(2024, 1, 1),
        observed_through=date(2024, 12, 31),
    )
    assert run.series_fallback == 2
    assert {record.model_name for record in run.forecasts} == {"trailing_3_mean"}
    assert {
        record.fallback_reason for record in run.forecasts if record.product_id == "a"
    } == {"candidate_population_validation_mae_worse_or_equal"}


def test_training_is_capped_to_latest_twelve_months_and_future_rows_rejected():
    rows = [
        {
            "product_id": "p",
            "warehouse_key": "w",
            "demand_date": f"{year}-{month:02d}-01",
            "ordered_quantity": 2,
        }
        for year, month in [(2024, month) for month in range(1, 13)]
        + [(2025, month) for month in range(1, 13)]
    ]
    run = run_forecast(
        rows,
        observed_through=date(2025, 12, 31),
        coverage_start=date(2024, 1, 1),
        generated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert run.training_start == date(2025, 1, 1)
    assert run.training_end == date(2025, 12, 1)
    with pytest.raises(ValueError, match="after observed_through"):
        run_forecast(
            rows + [{**rows[-1], "demand_date": "2026-01-01"}],
            observed_through=date(2025, 12, 31),
            coverage_start=date(2024, 1, 1),
        )


def test_incomplete_month_bounds_are_rejected():
    with pytest.raises(ValueError, match="last day"):
        run_forecast(
            [], observed_through=date(2024, 12, 30), coverage_start=date(2024, 1, 1)
        )
    with pytest.raises(ValueError, match="first day"):
        run_forecast(
            [], observed_through=date(2024, 12, 31), coverage_start=date(2024, 1, 2)
        )


def test_candidate_scoring_failure_falls_back_once(monkeypatch):
    rows = [
        {
            "product_id": "p",
            "warehouse_key": "w",
            "demand_date": f"2024-{month:02d}-01",
            "ordered_quantity": month,
        }
        for month in range(1, 13)
    ]

    def fail_fit(_values):
        raise RuntimeError("model fitting failed")

    monkeypatch.setattr(pipeline, "fit_damped_holt", fail_fit)
    result = run_forecast(
        rows,
        observed_through=date(2024, 12, 31),
        coverage_start=date(2024, 1, 1),
    )
    assert result.series_fallback == 1
    assert all(record.model_name == "trailing_3_mean" for record in result.forecasts)
    assert all(
        record.fallback_reason == "candidate_fit_failed" for record in result.forecasts
    )
    assert result.evaluations[0].candidate_mae is None
    assert result.evaluations[0].baseline_mae is not None


def test_sparse_series_uses_pooled_validation_intervals():
    rows = [
        {
            "product_id": "full",
            "warehouse_key": "w",
            "demand_date": f"2024-{month:02d}-01",
            "ordered_quantity": month % 4,
        }
        for month in range(1, 13)
    ] + [
        {
            "product_id": "sparse",
            "warehouse_key": "w",
            "demand_date": f"2024-{month:02d}-01",
            "ordered_quantity": 1,
        }
        for month in range(9, 13)
    ]
    result = run_forecast(
        rows,
        observed_through=date(2024, 12, 31),
        coverage_start=date(2024, 1, 1),
    )
    sparse = [record for record in result.forecasts if record.product_id == "sparse"]
    assert sparse
    assert sparse[0].uncertainty_method == "validation_abs_error_p90_pooled_by_lead"
    assert sparse[0].residual_sample_count > 0


def test_interval_calibration_is_per_lead_and_excludes_final_test_months():
    rows = [
        {
            "product_id": f"p{series}",
            "warehouse_key": "w",
            "demand_date": f"2024-{month:02d}-01",
            "ordered_quantity": month * series,
        }
        for series in range(1, 4)
        for month in range(1, 13)
    ]

    def forecast(input_rows):
        return run_forecast(
            input_rows,
            observed_through=date(2024, 12, 31),
            coverage_start=date(2024, 1, 1),
        )

    original = forecast(rows)
    changed_test = [
        {**row, "ordered_quantity": row["ordered_quantity"] + 1000}
        if row["demand_date"] >= "2024-10-01"
        else row
        for row in rows
    ]
    changed = forecast(changed_test)
    original_rows = [row for row in original.forecasts if row.product_id == "p1"]
    changed_rows = [row for row in changed.forecasts if row.product_id == "p1"]
    assert [row.residual_sample_count for row in original_rows] == [3, 6, 3]
    assert [row.uncertainty_method for row in original_rows] == [
        "validation_abs_error_p90_series_by_lead",
        "validation_abs_error_p90_pooled_by_lead",
        "validation_abs_error_p90_pooled_by_lead",
    ]
    original_widths = [
        row.upper_bound - row.predicted_demand
        for row in original_rows
        if row.upper_bound is not None
    ]
    changed_widths = [
        row.upper_bound - row.predicted_demand
        for row in changed_rows
        if row.upper_bound is not None
    ]
    assert original_widths[0] < original_widths[1] < original_widths[2]
    assert changed_widths == pytest.approx(original_widths)
