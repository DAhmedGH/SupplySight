"""Leakage-safe lag and rolling features computed from prior observations only."""

from __future__ import annotations

from typing import Sequence


def lag_features(
    values: Sequence[float], *, lags: tuple[int, ...] = (1, 2, 3)
) -> list[dict[str, float]]:
    output = []
    for index in range(len(values)):
        features = {
            f"lag_{lag}": float(values[index - lag]) if index >= lag else 0.0
            for lag in lags
        }
        prior = values[max(0, index - 3) : index]
        features["rolling_mean_3"] = sum(prior) / len(prior) if prior else 0.0
        output.append(features)
    return output
