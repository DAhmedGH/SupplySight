"""Command line interface for synthetic operational data generation."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date
from pathlib import Path

from supplysight.data_generation.config import PROFILES, config_for_profile
from supplysight.data_generation.generate import generate_dataset


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate linked SupplySight operational CSV data."
    )
    parser.add_argument("--profile", choices=sorted(PROFILES), default="dev")
    parser.add_argument("--seed", type=int, default=20240924)
    parser.add_argument(
        "--start-date", type=date.fromisoformat, default=date(2024, 1, 1)
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat, default=date(2024, 12, 31)
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--defect-rate", type=float, default=0.01)
    parser.add_argument(
        "--inventory-interval-days",
        type=int,
        help="snapshot cadence in days (defaults to the selected profile)",
    )
    parser.add_argument(
        "--clean", action="store_true", help="disable all intentional defects"
    )
    args = parser.parse_args()
    config = config_for_profile(
        args.profile,
        seed=args.seed,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        defect_rate=args.defect_rate,
    )
    if args.inventory_interval_days is not None:
        config = replace(
            config, inventory_interval_days=args.inventory_interval_days
        )
    report = generate_dataset(config, clean=args.clean)
    print(f"Generated {sum(report['row_counts'].values()):,} rows in {args.output_dir}")
    print(f"Validation: {'passed' if report['validation']['valid'] else 'failed'}")
    print(f"Defects injected: {sum(report['defect_counts'].values()):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
