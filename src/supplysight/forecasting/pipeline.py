"""Run deterministic, per-series monthly forecasting and evaluation."""

from __future__ import annotations

import calendar
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from math import ceil
from typing import Any, Iterable, Mapping

from .dataset import DemandSeries, add_month, build_series, month_start
from .evaluation import Metrics, metrics, temporal_splits
from .models import fit_damped_holt, moving_average

MODEL_VERSION = "monthly_demand_v1"


@dataclass(frozen=True)
class ForecastConfig:
    minimum_months: int = 12
    minimum_nonzero: int = 4
    residual_quantile: float = 0.9


DEFAULT_CONFIG = ForecastConfig()


@dataclass(frozen=True)
class ForecastRecord:
    run_id: str
    generated_at: datetime
    forecast_date: date
    product_id: str
    product_key: str | None
    warehouse_key: str
    horizon: int
    predicted_demand: float
    lower_bound: float | None
    upper_bound: float | None
    model_name: str
    fallback: bool
    training_start: date
    training_end: date
    source_version: str | None
    fallback_reason: str | None
    uncertainty_method: str
    residual_sample_count: int
    model_version: str


@dataclass(frozen=True)
class EvaluationRecord:
    product_id: str
    warehouse_key: str
    model_name: str
    mae: float | None
    rmse: float | None
    wape: float | None
    selected: bool
    validation_period_start: date | None
    validation_period_end: date | None
    test_mae: float | None
    test_rmse: float | None
    test_wape: float | None
    baseline_mae: float | None
    baseline_rmse: float | None
    baseline_wape: float | None
    candidate_mae: float | None
    candidate_rmse: float | None
    candidate_wape: float | None


@dataclass(frozen=True)
class ForecastRun:
    run_id: str
    status: str
    started_at: datetime
    completed_at: datetime
    training_start: date | None
    training_end: date | None
    horizon: int
    series_attempted: int
    series_successful: int
    series_fallback: int
    series_failed: int
    records_produced: int
    input_fingerprint: str
    failure_reason: str | None
    forecasts: list[ForecastRecord]
    evaluations: list[EvaluationRecord]
    model_version: str


def _score(
    series: DemandSeries,
    method: str,
    origins: tuple[int, ...],
    horizon: int,
) -> tuple[Metrics | None, list[float]]:
    actuals: list[float] = []
    predictions: list[float] = []
    for origin in origins:
        actual = series.demand[origin : origin + 1]
        history = series.demand[:origin]
        if len(history) < 6:
            continue
        prediction = (
            moving_average(history, 1)
            if method == "trailing_3_mean"
            else fit_damped_holt(history).predict(1)
        )
        actuals.extend(actual)
        predictions.extend(prediction)
    result = metrics(actuals, predictions) if actuals else None
    residuals = [abs(a - p) for a, p in zip(actuals, predictions)]
    return result, residuals


def _lead_residuals(
    series: DemandSeries,
    method: str,
    origins: tuple[int, ...],
    horizon: int,
    test_start: int,
) -> dict[int, list[float]]:
    """Collect empirical errors by horizon lead without observing test targets."""
    result = {lead: [] for lead in range(1, horizon + 1)}
    validation_start = origins[0] if origins else test_start
    validation_end = validation_start + 3
    for origin in origins:
        history = series.demand[:origin]
        if len(history) < 6:
            continue
        try:
            forecast = (
                moving_average(history, horizon)
                if method == "trailing_3_mean"
                else fit_damped_holt(history).predict(horizon)
            )
        except Exception:
            continue
        for lead, predicted in enumerate(forecast, start=1):
            target_index = origin + lead - 1
            if target_index >= min(validation_end, test_start):
                continue
            result[lead].append(abs(series.demand[target_index] - predicted))
    return result


def _test_score(series: DemandSeries, method: str, horizon: int) -> Metrics | None:
    if len(series.demand) <= horizon:
        return None
    actual = list(series.demand[-horizon:])
    history = series.demand[:-horizon]
    try:
        predicted = (
            moving_average(history, horizon)
            if method == "trailing_3_mean"
            else fit_damped_holt(history).predict(horizon)
        )
        return metrics(actual, predicted)
    except Exception:
        return None


def _forecast(series: DemandSeries, method: str, horizon: int) -> list[float]:
    if method == "damped_holt":
        return fit_damped_holt(series.demand).predict(horizon)
    return moving_average(series.demand, horizon)


def _validate_month_bounds(coverage_start: date, observed_through: date) -> None:
    if coverage_start.day != 1:
        raise ValueError("coverage_start must be the first day of a month")
    month_end = calendar.monthrange(observed_through.year, observed_through.month)[1]
    if observed_through.day != month_end:
        raise ValueError("observed_through must be the last day of a month")
    if coverage_start > observed_through:
        raise ValueError("coverage_start must not follow observed_through")


def run_forecast(
    rows: Iterable[Mapping[str, Any]],
    *,
    run_id: str | None = None,
    generated_at: datetime | None = None,
    observed_through: date | None = None,
    coverage_start: date | None = None,
    horizon: int = 3,
    source_version: str | None = None,
) -> ForecastRun:
    """Forecast each product/warehouse series from eligible monthly order demand."""
    started = generated_at or datetime.now(timezone.utc)
    if horizon < 1:
        raise ValueError("horizon must be positive")
    if observed_through is None or coverage_start is None:
        raise ValueError("observed_through and coverage_start must be explicit")
    _validate_month_bounds(coverage_start, observed_through)
    materialized = [dict(row) for row in rows]
    materialized.sort(
        key=lambda row: json.dumps(
            row, sort_keys=True, default=str, separators=(",", ":")
        )
    )
    canonical = json.dumps(
        materialized, sort_keys=True, default=str, separators=(",", ":")
    )
    fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
    run_id = run_id or str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            fingerprint + str(observed_through) + str(horizon) + MODEL_VERSION,
        )
    )
    observed_month = month_start(observed_through)
    training_cutoff = add_month(observed_month, 1 - DEFAULT_CONFIG.minimum_months)
    bounded_rows = []
    for row in materialized:
        raw_period = row.get("month", row.get("demand_date", row.get("order_date")))
        period = month_start(raw_period)
        if period > observed_month:
            raise ValueError("Input contains demand after observed_through")
        if period < month_start(coverage_start):
            raise ValueError("Input contains demand before coverage_start")
        if period >= training_cutoff:
            bounded_rows.append(row)
    series_list = build_series(
        bounded_rows,
        observed_through=observed_through,
        coverage_start=training_cutoff,
    )
    forecasts: list[ForecastRecord] = []
    evaluations: list[EvaluationRecord] = []
    fallback_count = failed_count = 0
    starts = [s.months[0] for s in series_list if s.months]
    ends = [s.months[-1] for s in series_list if s.months]
    decisions: list[dict[str, Any]] = []
    pooled_errors: list[float] = []
    for series in series_list:
        n = len(series.demand)
        splits = temporal_splits(n)
        candidate_eligible = (
            n >= DEFAULT_CONFIG.minimum_months
            and sum(value > 0 for value in series.demand)
            >= DEFAULT_CONFIG.minimum_nonzero
        )
        baseline_name = "trailing_3_mean"
        baseline_validation, baseline_residuals = None, []
        if splits:
            try:
                baseline_validation, baseline_residuals = _score(
                    series, baseline_name, splits, horizon
                )
            except Exception:
                baseline_validation = None
        candidate_validation, candidate_residuals = (None, [])
        candidate_error = None
        if candidate_eligible and splits:
            try:
                candidate_validation, candidate_residuals = _score(
                    series, "damped_holt", splits, horizon
                )
            except Exception as exc:
                candidate_error = str(exc)
        selected = (
            "damped_holt"
            if candidate_validation is not None
            and baseline_validation is not None
            and candidate_validation.mae < baseline_validation.mae
            else baseline_name
        )
        test = _test_score(series, selected, horizon)
        chosen_validation = (
            candidate_validation if selected == "damped_holt" else baseline_validation
        )
        chosen_resid = (
            candidate_residuals if selected == "damped_holt" else baseline_residuals
        )
        decisions.append(
            {
                "series": series,
                "splits": splits,
                "selected": selected,
                "candidate_eligible": candidate_eligible,
                "candidate_error": candidate_error,
                "baseline_validation": baseline_validation,
                "baseline_residuals": baseline_residuals,
                "candidate_validation": candidate_validation,
                "chosen_validation": chosen_validation,
                "chosen_resid": chosen_resid,
                "test": test,
            }
        )

    paired = [
        decision
        for decision in decisions
        if decision["baseline_validation"] is not None
        and decision["candidate_validation"] is not None
    ]
    candidate_population_allowed = bool(paired) and sum(
        decision["candidate_validation"].mae for decision in paired
    ) < sum(decision["baseline_validation"].mae for decision in paired)
    if not candidate_population_allowed:
        for decision in decisions:
            if decision["selected"] == "damped_holt":
                decision["selected"] = "trailing_3_mean"
                decision["chosen_validation"] = decision["baseline_validation"]
                decision["chosen_resid"] = decision["baseline_residuals"]
                decision["test"] = _test_score(
                    decision["series"], "trailing_3_mean", horizon
                )
                decision["population_gate_fallback"] = True

    # Resolve final fits before collecting residuals so calibration follows the
    # model that will actually generate the future forecast.
    for decision in decisions:
        series = decision["series"]
        selected = decision["selected"]
        try:
            decision["prediction"] = _forecast(series, selected, horizon)
        except Exception:
            if selected == "damped_holt":
                decision["selected"] = "trailing_3_mean"
                decision["chosen_validation"] = decision["baseline_validation"]
                decision["chosen_resid"] = decision["baseline_residuals"]
                decision["test"] = _test_score(series, "trailing_3_mean", horizon)
                decision["final_fit_fallback"] = True
            decision["prediction"] = moving_average(series.demand, horizon)
        decision["lead_residuals"] = _lead_residuals(
            series,
            decision["selected"],
            decision["splits"],
            horizon,
            max(0, len(series.demand) - 3),
        )

    pooled_by_lead = {
        lead: [
            value
            for decision in decisions
            for value in decision["lead_residuals"][lead]
        ]
        for lead in range(1, horizon + 1)
    }

    for decision in decisions:
        series = decision["series"]
        selected = decision["selected"]
        fallback_reason = None
        if selected == "trailing_3_mean":
            fallback_count += 1
            if len(series.demand) < DEFAULT_CONFIG.minimum_months:
                fallback_reason = "insufficient_history"
            elif (
                sum(value > 0 for value in series.demand)
                < DEFAULT_CONFIG.minimum_nonzero
            ):
                fallback_reason = "insufficient_nonzero_periods"
            elif decision["candidate_error"]:
                fallback_reason = "candidate_fit_failed"
            elif decision.get("population_gate_fallback"):
                fallback_reason = "candidate_population_validation_mae_worse_or_equal"
            elif decision.get("final_fit_fallback"):
                fallback_reason = "candidate_final_fit_failed"
            elif decision["candidate_validation"] is None:
                fallback_reason = "validation_unavailable"
            else:
                fallback_reason = "baseline_validation_mae_better_or_equal"
        selected = decision["selected"]
        prediction = decision["prediction"]
        used_fallback = selected == "trailing_3_mean"
        product_key = next(
            (
                str(r["product_key"])
                for r in materialized
                if str(r.get("product_id")) == series.key.product_id
                and str(r.get("warehouse_key")) == series.key.warehouse_key
                and r.get("product_key") is not None
            ),
            None,
        )
        training_start, training_end = series.months[0], series.months[-1]
        for step, value in enumerate(prediction, 1):
            series_errors = decision["lead_residuals"][step]
            pooled_errors = pooled_by_lead[step]
            if len(series_errors) >= 3:
                residuals = series_errors
                interval_method = "validation_abs_error_p90_series_by_lead"
            elif len(pooled_errors) >= 3:
                residuals = pooled_errors
                interval_method = "validation_abs_error_p90_pooled_by_lead"
            else:
                residuals = []
                interval_method = "unavailable_insufficient_lead_samples"
            error = (
                sorted(residuals)[
                    min(
                        len(residuals) - 1,
                        ceil(DEFAULT_CONFIG.residual_quantile * len(residuals)) - 1,
                    )
                ]
                if residuals
                else None
            )
            period = add_month(training_end, step)
            lower = max(0.0, value - error) if error is not None else None
            upper = max(value, value + error) if error is not None else None
            forecasts.append(
                ForecastRecord(
                    run_id,
                    started,
                    period,
                    series.key.product_id,
                    product_key,
                    series.key.warehouse_key,
                    step,
                    max(0.0, value),
                    lower,
                    upper,
                    selected,
                    used_fallback,
                    training_start,
                    training_end,
                    source_version,
                    fallback_reason,
                    interval_method,
                    len(residuals),
                    MODEL_VERSION,
                )
            )
        splits = decision["splits"]
        n = len(series.demand)
        val_start = series.months[splits[0]] if splits else None
        val_end = series.months[splits[-1]] if splits else None
        chosen_validation = decision["chosen_validation"]
        baseline_validation = decision["baseline_validation"]
        candidate_validation = decision["candidate_validation"]
        test = decision["test"]
        evaluations.append(
            EvaluationRecord(
                series.key.product_id,
                series.key.warehouse_key,
                selected,
                chosen_validation.mae if chosen_validation else None,
                chosen_validation.rmse if chosen_validation else None,
                chosen_validation.wape if chosen_validation else None,
                selected == "damped_holt",
                val_start,
                val_end,
                test.mae if test else None,
                test.rmse if test else None,
                test.wape if test else None,
                baseline_validation.mae if baseline_validation else None,
                baseline_validation.rmse if baseline_validation else None,
                baseline_validation.wape if baseline_validation else None,
                candidate_validation.mae if candidate_validation else None,
                candidate_validation.rmse if candidate_validation else None,
                candidate_validation.wape if candidate_validation else None,
            )
        )
    forecasts.sort(key=lambda r: (r.product_id, r.warehouse_key, r.forecast_date))
    evaluations.sort(key=lambda r: (r.product_id, r.warehouse_key))
    completed = datetime.now(timezone.utc)
    return ForecastRun(
        run_id,
        "SUCCESS" if series_list else "FAILED",
        started,
        completed,
        min(starts) if starts else None,
        max(ends) if ends else None,
        horizon,
        len(series_list),
        len(series_list),
        fallback_count,
        failed_count,
        len(forecasts),
        fingerprint,
        None if series_list else "No eligible demand series",
        forecasts,
        evaluations,
        MODEL_VERSION,
    )
