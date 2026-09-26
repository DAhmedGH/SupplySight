"""Normalize eligible warehouse demand into complete monthly series."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, order=True)
class SeriesKey:
    product_id: str
    warehouse_key: str


@dataclass(frozen=True)
class DemandSeries:
    key: SeriesKey
    months: tuple[date, ...]
    demand: tuple[float, ...]


def month_start(value: Any) -> date:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.replace(day=1)
    parsed = date.fromisoformat(str(value)[:10])
    return parsed.replace(day=1)


def add_month(value: date, count: int = 1) -> date:
    ordinal = value.year * 12 + value.month - 1 + count
    return date(ordinal // 12, ordinal % 12 + 1, 1)


def build_series(
    rows: Iterable[Mapping[str, Any]],
    *,
    observed_through: date | None = None,
    coverage_start: date | None = None,
) -> list[DemandSeries]:
    """Aggregate gross ordered quantities; WARNING is included, quarantine excluded.

    Missing months inside each product/warehouse's observed coverage are explicit
    zeroes. Demand is never forward-filled.
    """
    totals: dict[SeriesKey, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    ends: dict[SeriesKey, date] = {}
    starts: dict[SeriesKey, date] = {}
    for row in rows:
        status = str(
            row.get("data_quality_status", row.get("quality_status", "VALID"))
        ).upper()
        if status == "QUARANTINED":
            continue
        key = SeriesKey(str(row["product_id"]), str(row["warehouse_key"]))
        period = month_start(
            row.get("month", row.get("demand_date", row.get("order_date")))
        )
        if observed_through is not None and period > month_start(observed_through):
            raise ValueError("Input contains demand after observed_through")
        if coverage_start is not None and period < month_start(coverage_start):
            raise ValueError("Input contains demand before coverage_start")
        qty = float(row.get("ordered_quantity", row.get("quantity", 0)) or 0)
        if not isfinite(qty):
            raise ValueError("Demand quantity must be finite")
        if qty < 0:
            raise ValueError("Gross ordered demand cannot be negative")
        totals[key][period] += qty
        starts[key] = min(starts.get(key, period), period)
        ends[key] = max(ends.get(key, period), period)
    result = []
    for key in sorted(totals):
        # Demand before the first observed product/warehouse row is unknown, not zero.
        start = starts[key]
        if coverage_start and start < month_start(coverage_start):
            raise ValueError("Series begins before declared coverage_start")
        end = month_start(observed_through) if observed_through else ends[key]
        if start > end:
            raise ValueError("coverage_start must not follow observed_through")
        periods = []
        current = start
        while current <= end:
            periods.append(current)
            current = add_month(current)
        result.append(
            DemandSeries(
                key, tuple(periods), tuple(totals[key].get(p, 0.0) for p in periods)
            )
        )
    return result
