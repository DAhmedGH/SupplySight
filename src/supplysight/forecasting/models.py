"""Forecast baselines and a fixed-configuration damped Holt model."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Sequence


@dataclass(frozen=True)
class FittedHolt:
    level: float
    trend: float
    alpha: float = 0.35
    beta: float = 0.15
    damping: float = 0.8

    def predict(self, horizon: int) -> list[float]:
        values, damp, cumulative_trend = [], self.damping, 0.0
        for _ in range(horizon):
            cumulative_trend += damp
            values.append(max(0.0, self.level + self.trend * cumulative_trend))
            damp *= self.damping
        return values


def moving_average(
    values: Sequence[float], horizon: int = 3, window: int = 3
) -> list[float]:
    avg = mean(values[-window:]) if values else 0.0
    return [max(0.0, avg)] * horizon


def fit_damped_holt(values: Sequence[float]) -> FittedHolt:
    if len(values) < 2:
        raise ValueError("Holt model requires at least two observations")
    alpha, beta, damping = 0.35, 0.15, 0.8
    level, trend = float(values[0]), float(values[1]) - float(values[0])
    for observation in values[1:]:
        observation = float(observation)
        previous = level
        level = alpha * observation + (1 - alpha) * (level + damping * trend)
        trend = beta * (level - previous) + (1 - beta) * damping * trend
    return FittedHolt(max(0.0, level), trend, alpha, beta, damping)
