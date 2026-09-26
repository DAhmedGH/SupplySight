"""Temporal validation and scale-aware forecast metrics."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Sequence


@dataclass(frozen=True)
class Metrics:
    mae: float
    rmse: float
    wape: float | None


def metrics(actual: Sequence[float], predicted: Sequence[float]) -> Metrics:
    if len(actual) != len(predicted) or not actual:
        raise ValueError("actual and predicted must have equal nonzero length")
    errors = [float(a) - float(p) for a, p in zip(actual, predicted)]
    mae = sum(abs(e) for e in errors) / len(errors)
    rmse = sqrt(sum(e * e for e in errors) / len(errors))
    denominator = sum(abs(float(a)) for a in actual)
    return Metrics(
        mae, rmse, sum(abs(e) for e in errors) / denominator if denominator else None
    )


def temporal_splits(length: int) -> tuple[int, ...]:
    """Return one-step rolling origins inside the three-month validation block.

    Every origin has at least six complete training months. The final three months
    are excluded from these validation origins and reserved for the test.
    """
    if length < 12:
        return ()
    validation_start = length - 6
    return tuple(range(validation_start, validation_start + 3))
