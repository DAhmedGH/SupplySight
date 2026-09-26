"""Command line entry point for a DEV Snowflake forecast run."""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from dotenv import load_dotenv

from supplysight.forecasting import run_forecast
from supplysight.forecasting.pipeline import MODEL_VERSION
from supplysight.forecasting.repository import load_monthly_demand, persist_run
from supplysight.settings import SnowflakeSettings

logger = logging.getLogger(__name__)


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build demand forecasts from SUPPLY_CHAIN_DEV analytical marts."
    )
    parser.add_argument("--coverage-start", required=True, type=_date)
    parser.add_argument("--observed-through", required=True, type=_date)
    parser.add_argument("--horizon", type=int, default=3)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--source-version", default=None)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate and print counts without writing forecast outputs.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.coverage_start > args.observed_through:
        raise SystemExit("--coverage-start must not be after --observed-through")
    if args.coverage_start.day != 1:
        raise SystemExit("--coverage-start must be the first day of a month")
    month_end = calendar.monthrange(
        args.observed_through.year, args.observed_through.month
    )[1]
    if args.observed_through.day != month_end:
        raise SystemExit("--observed-through must be the last day of a month")
    if args.horizon < 1:
        raise SystemExit("--horizon must be at least 1")

    load_dotenv(args.env_file, override=False)
    settings = SnowflakeSettings.from_environment()
    rows = load_monthly_demand(settings, args.coverage_start, args.observed_through)
    ordered_rows = sorted(
        (dict(row) for row in rows),
        key=lambda row: json.dumps(
            row, sort_keys=True, default=str, separators=(",", ":")
        ),
    )
    fingerprint = hashlib.sha256(
        json.dumps(
            ordered_rows, sort_keys=True, default=str, separators=(",", ":")
        ).encode()
    ).hexdigest()
    deterministic_id = str(
        uuid5(
            NAMESPACE_URL,
            fingerprint
            + str(args.observed_through)
            + str(args.horizon)
            + MODEL_VERSION,
        )
    )
    try:
        result = run_forecast(
            ordered_rows,
            run_id=args.run_id,
            observed_through=args.observed_through,
            coverage_start=args.coverage_start,
            horizon=args.horizon,
            source_version=args.source_version,
        )
    except Exception as exc:
        if not args.dry_run:
            failure_id = args.run_id or deterministic_id
            failed_at = datetime.now(timezone.utc)
            try:
                persist_run(
                    settings,
                    run_id=failure_id,
                    forecasts=[],
                    evaluations=[],
                    run_metadata={
                        "started_at": failed_at,
                        "completed_at": datetime.now(timezone.utc),
                        "training_start": None,
                        "training_end": None,
                        "horizon": args.horizon,
                        "series_attempted": 0,
                        "series_successful": 0,
                        "series_fallback": 0,
                        "series_failed": 0,
                        "records_produced": 0,
                        "input_fingerprint": fingerprint,
                    },
                    status="FAILED",
                    failure_reason=str(exc)[:2000],
                    source_version=args.source_version,
                    coverage_start=args.coverage_start,
                    observed_through=args.observed_through,
                )
            except Exception:
                logger.exception("Could not record failed forecast run %s", failure_id)
        raise

    logger.info(
        "Forecast run %s: status=%s series=%s succeeded=%s "
        "fallback=%s failed=%s forecasts=%s",
        result.run_id,
        result.status,
        result.series_attempted,
        result.series_successful,
        result.series_fallback,
        result.series_failed,
        result.records_produced,
    )
    if not args.dry_run:
        persist_run(
            settings,
            run_id=result.run_id,
            forecasts=result.forecasts,
            evaluations=result.evaluations,
            run_metadata=result,
            status=result.status,
            failure_reason=result.failure_reason,
            source_version=args.source_version,
            coverage_start=args.coverage_start,
            observed_through=args.observed_through,
        )
        logger.info("Forecast outputs committed for run %s", result.run_id)
    else:
        logger.info("Dry run: no forecast tables were modified")
    return 0 if result.status.upper() in {"SUCCESS", "SUCCEEDED", "PARTIAL"} else 1
