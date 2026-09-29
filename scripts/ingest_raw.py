"""Validate synthetic CSV files and load them into the development RAW schema."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from supplysight.ingestion import PreflightError, ingest_dataset, preflight_dataset
from supplysight.ingestion.preflight import write_quarantine_sidecar
from supplysight.settings import ConfigurationError, SnowflakeSettings

LOGGER = logging.getLogger("supplysight.ingest_raw")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load the nine operational CSV files into SUPPLY_CHAIN_DEV.RAW."
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data/generated/dev"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--quarantine-dir", type=Path, default=Path("data/quarantine"))
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    load_dotenv(args.env_file, override=False)
    try:
        validation = preflight_dataset(args.input_dir)
    except PreflightError as exc:
        quarantine_file = write_quarantine_sidecar(
            exc.sidecar, args.quarantine_dir / "preflight_rejects.jsonl"
        )
        LOGGER.error("Preflight failed before Snowflake access: %s", exc)
        if quarantine_file:
            LOGGER.error("Rejected source rows were written to %s", quarantine_file)
        return 2
    try:
        settings = SnowflakeSettings.from_environment()
        result = ingest_dataset(args.input_dir, settings, preflight=validation)
    except (ConfigurationError, RuntimeError, OSError) as exc:
        LOGGER.error("RAW ingestion failed: %s", exc)
        return 1
    except Exception as exc:
        # Avoid printing connector exception details that can contain account data.
        LOGGER.error("RAW ingestion failed (%s)", type(exc).__name__)
        return 1
    LOGGER.info(
        "Batch %s succeeded: rows=%d inserted=%d updated=%d "
        "unchanged=%d warnings=%d quarantined=%d",
        result.batch_id,
        result.source_rows,
        result.inserted_rows,
        result.updated_rows,
        result.unchanged_rows,
        result.warning_rows,
        result.quarantined_rows,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
