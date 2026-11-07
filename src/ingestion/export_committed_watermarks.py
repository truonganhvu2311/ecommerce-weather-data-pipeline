"""Day 8 / one-time bootstrap export for the Airflow incremental extractor.

Reads BigQuery only. --check makes NO local changes; --export writes a new
ignored local JSON checkpoint file (never overwrites an existing file).

Designed for the verified 2026-11-03 initial GCS snapshot bootstrap, NOT a
recurring export/commit mechanism for production Airflow.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ID = "weather-data-project-512207"
LOCATION = "asia-southeast1"
EXPECTED_TABLES = {"customers", "products", "orders", "order_items"}
EXPECTED_BOOTSTRAP_ID = "bootstrap_gcs_snapshot_20261103"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = PROJECT_ROOT / "state" / "airflow" / "committed_watermarks_verified.json"

CHECKPOINT_SQL = f"""
SELECT source_table, committed_watermark, committed_batch_id
FROM `{PROJECT_ID}.de_etl.watermarks`
ORDER BY source_table
"""
RAW_BOUNDARIES_SQL = f"""
SELECT 'customers' AS source_table, MAX(_watermark_to) AS max_raw_watermark
FROM `{PROJECT_ID}.de_raw.customers`
UNION ALL
SELECT 'products', MAX(_watermark_to)
FROM `{PROJECT_ID}.de_raw.products`
UNION ALL
SELECT 'orders', MAX(_watermark_to)
FROM `{PROJECT_ID}.de_raw.orders`
UNION ALL
SELECT 'order_items', MAX(_watermark_to)
FROM `{PROJECT_ID}.de_raw.order_items`
"""


def require_aware_utc(dt: datetime, field: str) -> datetime:
    if not isinstance(dt, datetime) or dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"Missing or timezone-naive timestamp: {field}")
    return dt.astimezone(timezone.utc)


def get_verified_values(client) -> dict[str, str]:
    checkpoints = list(client.query(CHECKPOINT_SQL, location=LOCATION).result())
    if len(checkpoints) != 4:
        raise RuntimeError(f"Expected exactly 4 committed checkpoint rows, got {len(checkpoints)}")

    found = {}
    for row in checkpoints:
        table = row.source_table
        if table not in EXPECTED_TABLES or table in found:
            raise RuntimeError(f"Unexpected or duplicated source_table: {table}")
        if row.committed_batch_id != EXPECTED_BOOTSTRAP_ID:
            raise RuntimeError(f"Unexpected committed_batch_id for {table}: {row.committed_batch_id}")
        found[table] = require_aware_utc(row.committed_watermark, table)
    if set(found) != EXPECTED_TABLES:
        raise RuntimeError(f"Incorrect table set: {set(found)}")

    boundaries = list(client.query(RAW_BOUNDARIES_SQL, location=LOCATION).result())
    if len(boundaries) != 4:
        raise RuntimeError("Expected exactly 4 RAW watermark boundary rows")
    raw_seen = set()
    for row in boundaries:
        table = row.source_table
        if table not in EXPECTED_TABLES or table in raw_seen:
            raise RuntimeError(f"Unexpected or duplicate RAW boundary: {table}")
        raw_seen.add(table)
        raw_time = require_aware_utc(row.max_raw_watermark, f"raw.{table}")
        if found[table] != raw_time:
            raise RuntimeError(
                f"Checkpoint/RAW mismatch for {table}: {found[table].isoformat()} vs {raw_time.isoformat()}"
            )

    print("CHECK PASSED: 4 unique committed checkpoints match 4 BigQuery RAW boundaries")
    for table in sorted(found):
        print(f"  {table}: {found[table].isoformat()}")
    return {table: found[table].isoformat() for table in sorted(found)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Read-only BigQuery check; no local write")
    mode.add_argument("--export", action="store_true", help="Verify then create a new local JSON file")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    # Import lazily: calling --help does not need cloud dependencies.
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    mapping = get_verified_values(client)
    if args.check:
        print("No files written; no BigQuery data modified.")
        return

    output = args.output.resolve()
    if output == (PROJECT_ROOT / "state" / "watermark.json").resolve():
        raise RuntimeError("Refusing to overwrite legacy state/watermark.json")
    if output.exists():
        raise FileExistsError(f"Destination already exists; no overwrite: {output}")

    doc = {
        "source": "de_etl.watermarks",
        "verified": True,
        "project_id": PROJECT_ID,
        "bootstrap_batch_id": EXPECTED_BOOTSTRAP_ID,
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "committed_watermarks": mapping,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive create: never silently replace an exported checkpoint.
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(doc, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(f"EXPORT PASSED: {output}")
    print("Only a local JSON file was created; BigQuery and legacy watermark were unchanged.")


if __name__ == "__main__":
    main()
