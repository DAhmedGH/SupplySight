"""Configuration and scale profiles for local synthetic data generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class GenerationConfig:
    profile: str = "dev"
    seed: int = 20240924
    start_date: date = date(2024, 1, 1)
    end_date: date = date(2024, 12, 31)
    output_dir: Path = Path("data/generated")
    product_count: int = 40
    warehouse_count: int = 3
    supplier_count: int = 12
    customer_count: int = 500
    order_line_count: int = 5_000
    inventory_interval_days: int = 1
    defect_rate: float = 0.01

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if self.inventory_interval_days < 1:
            raise ValueError("inventory_interval_days must be at least 1")
        if not 0 <= self.defect_rate <= 1:
            raise ValueError("defect_rate must be between 0 and 1")
        if (
            min(
                self.product_count,
                self.warehouse_count,
                self.supplier_count,
                self.customer_count,
            )
            < 1
        ):
            raise ValueError("entity counts must be positive")
        if self.order_line_count < 0:
            raise ValueError("order_line_count must be non-negative")


PROFILES: dict[str, dict[str, int]] = {
    "dev": {
        "product_count": 40,
        "warehouse_count": 3,
        "supplier_count": 12,
        "customer_count": 500,
        "order_line_count": 5_000,
        "inventory_interval_days": 1,
    },
    "portfolio": {
        "product_count": 250,
        "warehouse_count": 8,
        "supplier_count": 40,
        "customer_count": 10_000,
        "order_line_count": 250_000,
        "inventory_interval_days": 7,
    },
}


def config_for_profile(
    profile: str,
    *,
    seed: int = 20240924,
    start_date: date = date(2024, 1, 1),
    end_date: date = date(2024, 12, 31),
    output_dir: Path = Path("data/generated"),
    defect_rate: float = 0.01,
) -> GenerationConfig:
    """Build a validated configuration from a named scale profile."""
    try:
        values = PROFILES[profile]
    except KeyError as exc:
        raise ValueError(
            f"unknown profile {profile!r}; choose from {', '.join(PROFILES)}"
        ) from exc
    return GenerationConfig(
        profile=profile,
        seed=seed,
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        defect_rate=defect_rate,
        **values,
    )
