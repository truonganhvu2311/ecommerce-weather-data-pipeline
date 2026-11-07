"""Prepare an immutable PostgreSQL incremental batch for a FUTURE Airflow DAG.

This script deliberately NEVER updates local state/watermark.json, GCS or BigQuery.
It does NOT bootstrap BigQuery's de_etl.watermarks. Use --self-check safely first.

After a separately verified checkpoint bootstrap, --prepare can read an explicit
JSON file exported from committed de_etl.watermarks. A batch is prepared once;
retries reuse its manifest and files instead of re-extracting the source.

This is a timestamp-based incremental exercise, NOT WAL CDC. In particular,
late-committing transactions with old updated_at values remain a limitation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
LEGACY_STATE_FILE = PROJECT_ROOT / "state" / "watermark.json"  # DO NOT WRITE
BATCH_ROOT = PROJECT_ROOT / "data" / "airflow_batches"
TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}
BATCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def utc_timestamp(text: str) -> datetime:
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Watermark must include a timezone/UTC offset")
    return value.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def read_verified_checkpoints(path: Path) -> dict[str, datetime]:
    """Require an explicitly verified committed checkpoint export, never legacy state."""
    if path.resolve() == LEGACY_STATE_FILE.resolve():
        raise ValueError("Refusing legacy state/watermark.json as a committed checkpoint")
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("source") != "de_etl.watermarks" or doc.get("verified") is not True:
        raise ValueError("Checkpoint JSON must be independently verified against BigQuery")
    mapping = doc.get("committed_watermarks")
    if not isinstance(mapping, dict) or set(mapping) != set(TABLES):
        raise ValueError("Require exactly four committed per-table watermarks")
    return {table: utc_timestamp(mapping[table]) for table in TABLES}


def read_existing_manifest(batch_dir: Path, base: dict[str, datetime]) -> dict:
    path = batch_dir / "manifest.json"
    if not path.is_file():
        raise RuntimeError(f"Batch directory exists without manifest; audit manually: {batch_dir}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    expected = {t: iso_utc(v) for t, v in base.items()}
    if manifest.get("watermark_from") != expected or manifest.get("status") != "pending":
        raise RuntimeError("Existing batch differs from committed checkpoint or is not pending")
    for item in manifest.get("files", []):
        relative = Path(item["local_path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("Unsafe local file path in manifest")
        payload = (batch_dir / relative).read_bytes()
        if hashlib.sha256(payload).hexdigest() != item["sha256"]:
            raise RuntimeError(f"Existing batch file changed: {relative}")
        if len(payload) != item["size_bytes"]:
            raise RuntimeError(f"Existing batch file size changed: {relative}")
    return manifest


def prepare(batch_id: str, checkpoints_file: Path, lag_minutes: int) -> None:
    if not BATCH_RE.fullmatch(batch_id):
        raise ValueError("batch_id must be 1-64 characters [A-Za-z0-9_-], starting alphanumeric")
    if not 0 <= lag_minutes <= 1440:
        raise ValueError("cutoff-lag-minutes must be between 0 and 1440")

    base = read_verified_checkpoints(checkpoints_file)
    batch_dir = BATCH_ROOT / batch_id
    if batch_dir.exists():
        existing = read_existing_manifest(batch_dir, base)
        print(f"REPLAY READY: existing immutable batch {batch_id}")
        print(f"Manifest: {batch_dir / 'manifest.json'}")
        print(f"Files: {len(existing['files'])}; no new extraction performed")
        return

    # Lazy imports keep --self-check usable without installing PostgreSQL packages.
    import psycopg2
    from psycopg2 import sql
    from dotenv import load_dotenv

    load_dotenv(ENV_FILE)
    password = os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("PGPASSWORD missing; keep credentials in local .env only")

    BATCH_ROOT.mkdir(parents=True, exist_ok=True)
    tmp_dir = BATCH_ROOT / f".{batch_id}.tmp-{uuid.uuid4().hex}"
    tmp_dir.mkdir()

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "ecommerce_source"),
        user=os.getenv("PGUSER", "postgres"),
        password=password,
    )
    try:
        # Every table shares one consistent committed snapshot; source is read only.
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        with conn.cursor() as cursor:
            cursor.execute("SELECT CURRENT_TIMESTAMP")
            source_snapshot_time = cursor.fetchone()[0]
            upper = source_snapshot_time - timedelta(minutes=lag_minutes)
            if any(w >= upper for w in base.values()):
                raise RuntimeError("Committed checkpoint is not earlier than extraction cutoff")

            file_entries = []
            row_counts = {}
            run_date = upper.strftime("%Y-%m-%d")
            for table, primary_key in TABLES.items():
                query = sql.SQL(
                    "SELECT * FROM {table} "
                    "WHERE updated_at > %s AND updated_at <= %s "
                    "ORDER BY updated_at, {pk}"
                ).format(table=sql.Identifier(table), pk=sql.Identifier(primary_key))
                cursor.execute(query, (base[table], upper))
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                row_counts[table] = len(rows)
                if not rows:
                    print(f"[{table}] 0 new/changed rows")
                    continue

                relative = Path("raw") / "transactions" / table / f"dt={run_date}" / f"{table}_{batch_id}.jsonl"
                output_path = tmp_dir / relative
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("x", encoding="utf-8", newline="\n") as writer:
                    for row in rows:
                        record = dict(zip(columns, row))
                        record.update({
                            "_source_table": table,
                            "_extracted_at": iso_utc(source_snapshot_time),
                            "_watermark_from": iso_utc(base[table]),
                            "_watermark_to": iso_utc(upper),
                        })
                        writer.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                payload = output_path.read_bytes()
                file_entries.append({
                    "source_type": table,
                    "object_path": relative.as_posix(),
                    "local_path": relative.as_posix(),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "size_bytes": len(payload),
                    "record_count": len(rows),
                })
                print(f"[{table}] {len(rows)} rows -> {relative.as_posix()}")
        conn.commit()  # Ends the READ-ONLY transaction. Does not commit a checkpoint.

        manifest = {
            "schema_version": 1,
            "batch_id": batch_id,
            "status": "pending",  # Not uploaded, loaded, dbt-tested or committed.
            "source": "postgresql_timestamp_incremental",
            "source_snapshot_utc": iso_utc(source_snapshot_time),
            "cutoff_lag_minutes": lag_minutes,
            "watermark_from": {t: iso_utc(v) for t, v in base.items()},
            "watermark_to": {t: iso_utc(upper) for t in TABLES},
            "row_counts": row_counts,
            "files": file_entries,
        }
        (tmp_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        # A complete batch is published atomically at a fixed, retry-safe path.
        if batch_dir.exists():
            raise RuntimeError("Another worker created this batch; stop to prevent overlap")
        tmp_dir.rename(batch_dir)
        print(f"PENDING BATCH READY: {batch_dir / 'manifest.json'}")
        print("No GCS, BigQuery, dbt or legacy watermark modifications were performed")
    except Exception:
        conn.rollback()
        print(f"FAILED: inspect partial temporary batch if present: {tmp_dir}")
        raise
    finally:
        conn.close()


def self_check() -> None:
    assert len(TABLES) == 4
    assert BATCH_RE.fullmatch("day8_test_20261103")
    assert not BATCH_RE.fullmatch("../unsafe")
    assert iso_utc(utc_timestamp("2026-10-19T21:12:44+07:00")) == "2026-10-19T14:12:44+00:00"
    print("SELF-CHECK PASSED")
    print("Legacy state/watermark.json is NEVER written by this script")
    print("BigQuery de_etl.watermarks has to be independently bootstrapped before --prepare")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--self-check", action="store_true", help="No DB connection, no data mutation")
    modes.add_argument("--prepare", action="store_true", help="Read source and create immutable pending batch")
    parser.add_argument("--batch-id", help="Stable batch id, reused across retries")
    parser.add_argument("--checkpoints-json", type=Path, help="Verified export of committed BigQuery watermarks")
    parser.add_argument("--cutoff-lag-minutes", type=int, default=10)
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    if not args.batch_id or args.checkpoints_json is None:
        parser.error("--prepare requires --batch-id and --checkpoints-json")
    prepare(args.batch_id, args.checkpoints_json, args.cutoff_lag_minutes)


if __name__ == "__main__":
    main()
