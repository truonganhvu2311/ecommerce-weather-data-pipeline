"""Day 8.7.10: offline, read-only validation of an immutable incremental batch.

This tool does NOT connect to PostgreSQL, GCS, BigQuery, or Airflow; it never
writes files or advances watermarks. It verifies the pending batch produced by
extract_incremental_airflow.py against a previously verified checkpoint export.

Usage:
    python validate_batch_manifest.py --manifest PATH --checkpoints-json PATH
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

TABLES = {"customers", "products", "orders", "order_items"}
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def require(cond: bool, message: str) -> None:
    if not cond:
        raise ValueError(message)


def parse_timestamp(value: str, field: str) -> datetime:
    require(isinstance(value, str), f"{field} must be a timestamp string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp in {field}") from exc
    require(dt.tzinfo is not None and dt.utcoffset() is not None,
            f"{field} must include UTC offset")
    return dt.astimezone(timezone.utc)


def load_json(path: Path) -> dict:
    require(path.is_file(), f"File not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(data, dict), f"Expected JSON object: {path}")
    return data


def validate(manifest_path: Path, checkpoint_path: Path) -> dict:
    manifest_path = manifest_path.resolve()
    checkpoint_path = checkpoint_path.resolve()
    doc = load_json(manifest_path)
    checkpoint_doc = load_json(checkpoint_path)
    batch_dir = manifest_path.parent

    batch_id = doc.get("batch_id")
    require(isinstance(batch_id, str) and BATCH_ID_RE.fullmatch(batch_id) is not None,
            "Invalid batch_id")
    require(batch_dir.name == batch_id, "Manifest folder does not match batch_id")
    require(doc.get("schema_version") == 1, "Unsupported schema_version")
    require(doc.get("status") == "pending", "Expected pending batch (no mutations yet)")
    require(doc.get("source") == "postgresql_timestamp_incremental", "Wrong batch source")
    require(checkpoint_doc.get("source") == "de_etl.watermarks" and
            checkpoint_doc.get("verified") is True, "Checkpoint export is not verified")

    committed = checkpoint_doc.get("committed_watermarks")
    from_map = doc.get("watermark_from")
    to_map = doc.get("watermark_to")
    counts = doc.get("row_counts")
    require(all(isinstance(m, dict) and set(m) == TABLES
                for m in (committed, from_map, to_map, counts)),
            "Watermarks and row_counts must each contain exactly four source tables")

    snap = parse_timestamp(doc.get("source_snapshot_utc"), "source_snapshot_utc")
    lag = doc.get("cutoff_lag_minutes")
    require(type(lag) is int and 0 <= lag <= 1440, "Invalid cutoff lag")
    target_cutoff = snap - timedelta(minutes=lag)

    for table in sorted(TABLES):
        committed_dt = parse_timestamp(committed[table], f"checkpoint.{table}")
        from_dt = parse_timestamp(from_map[table], f"watermark_from.{table}")
        to_dt = parse_timestamp(to_map[table], f"watermark_to.{table}")
        require(committed_dt == from_dt, f"Checkpoint mismatch for {table}")
        require(from_dt < to_dt, f"Invalid watermark range for {table}")
        require(to_dt == target_cutoff, f"Cutoff differs from source snapshot for {table}")
        require(type(counts[table]) is int and counts[table] >= 0,
                f"Invalid row count for {table}")

    files = doc.get("files")
    require(isinstance(files, list), "files must be an array")
    require(len(files) <= 4, "Expected at most one JSONL file per transaction table")
    seen = set()
    counted = {table: 0 for table in TABLES}
    for item in files:
        require(isinstance(item, dict), "Invalid file entry")
        table = item.get("source_type")
        require(table in TABLES, "Unexpected source_type")
        require(table not in seen, f"Multiple files for {table}")
        seen.add(table)
        raw_path = item.get("local_path")
        require(isinstance(raw_path, str) and item.get("object_path") == raw_path,
                "object_path/local_path mismatch")
        # Manifest file names come from the original immutable batch extractor.
        expected_pattern = (
            rf"raw/transactions/{re.escape(table)}/dt=\d{{4}}-\d{{2}}-\d{{2}}/"
            rf"{re.escape(table)}_{re.escape(batch_id)}\.jsonl"
        )
        require(re.fullmatch(expected_pattern, raw_path) is not None,
                f"Unexpected file path for {table}")
        relative = Path(raw_path)
        require(not relative.is_absolute() and ".." not in relative.parts,
                "Unsafe relative file path")
        local_file = (batch_dir / relative).resolve()
        require(local_file.is_relative_to(batch_dir), "File escapes batch directory")
        require(local_file.is_file(), f"Missing local file: {local_file}")
        contents = local_file.read_bytes()
        require(len(contents) == item.get("size_bytes"), f"Size mismatch: {raw_path}")
        require(hashlib.sha256(contents).hexdigest() == item.get("sha256"),
                f"SHA256 mismatch: {raw_path}")
        lines = contents.decode("utf-8-sig").splitlines()
        require(len(lines) > 0 and all(line.strip() for line in lines),
                f"Empty JSONL file or row: {raw_path}")
        require(type(item.get("record_count")) is int and
                len(lines) == item["record_count"], f"Record count mismatch: {raw_path}")
        for index, line in enumerate(lines, 1):
            record = json.loads(line)
            require(isinstance(record, dict) and record.get("_source_table") == table,
                    f"Wrong _source_table in {raw_path}:{index}")
            require(parse_timestamp(record.get("_watermark_from"), "record._watermark_from") ==
                    parse_timestamp(from_map[table], f"watermark_from.{table}"),
                    f"Wrong from watermark in {raw_path}:{index}")
            require(parse_timestamp(record.get("_watermark_to"), "record._watermark_to") ==
                    parse_timestamp(to_map[table], f"watermark_to.{table}"),
                    f"Wrong to watermark in {raw_path}:{index}")
            updated_at = parse_timestamp(record.get("updated_at"), "record.updated_at")
            require(parse_timestamp(from_map[table], "from") < updated_at <=
                    parse_timestamp(to_map[table], "to"),
                    f"Updated_at outside batch range in {raw_path}:{index}")
        counted[table] = len(lines)

    require(counted == counts, "Manifest row_counts do not match actual JSONL files")
    total_rows = sum(counts.values())
    gcs_prefix = f"airflow_batches/{batch_id}/"
    print(f"BATCH MANIFEST PREFLIGHT PASS: {batch_id}")
    print(f"status=pending, files={len(files)}, records={total_rows}")
    print("All local JSONL files and SHA-256 checksums verified")
    print(f"Future isolated GCS prefix: gs://weather-data-project-512207-raw/{gcs_prefix}")
    if not files:
        print("EMPTY_BATCH: no new RAW files; skip GCS upload and RAW loader")
    print("READ-ONLY: No PostgreSQL, GCS, BigQuery or watermark changes")
    return {"batch_id": batch_id, "files": len(files), "records": total_rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--checkpoints-json", required=True, type=Path)
    args = parser.parse_args()
    validate(args.manifest, args.checkpoints_json)


if __name__ == "__main__":
    main()
