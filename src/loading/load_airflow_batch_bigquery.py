"""Day 8.7.10.7: replay-safe PostgreSQL incremental batch -> BigQuery RAW.

Requires existing src/ingestion/validate_batch_manifest.py.

--self-test  Offline pure-function checks (no Google libraries or writes).
--check      Validate immutable local + GCS batch, LIVE BigQuery checkpoints,
             RAW schema, RAW/registry state. Read-only (may create query jobs).
--apply      Recheck everything; stage each file and atomically insert RAW +
             de_etl.loaded_files registry. Never updates pipeline_batches or
             de_etl.watermarks, never invokes dbt, never modifies GCS.

Run one loader per BigQuery project at a time. No concurrent ingest workers.
Leave legacy load_raw_to_bigquery.py unchanged: it is for the old 11-file migration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from types import SimpleNamespace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

PROJECT_ID = "weather-data-project-512207"
BUCKET = "weather-data-project-512207-raw"
LOCATION = "asia-southeast1"
TABLES = {"customers", "products", "orders", "order_items"}
SOURCE_COLUMNS = {
    "customers": ["customer_id", "customer_name", "city", "created_at", "updated_at"],
    "products": ["product_id", "product_name", "category", "price", "created_at", "updated_at"],
    "orders": ["order_id", "customer_id", "order_date", "status", "city",
               "total_amount", "created_at", "updated_at", "is_deleted"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity",
                    "unit_price", "created_at", "updated_at"],
}
META_COLUMNS = ["_source_table", "_extracted_at", "_watermark_from", "_watermark_to"]
ENRICH_COLUMNS = ["source_file", "source_line_number", "gcs_uri", "batch_id"]
DECIMAL_COLUMNS = {"price", "total_amount", "unit_price"}
KEY_COLUMNS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_item_id", "order_id", "product_id"],
}
REGISTRY_ID = f"{PROJECT_ID}.de_etl.loaded_files"
CHECKPOINT_ID = f"{PROJECT_ID}.de_etl.watermarks"


def must(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def timestamp_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    must(dt.tzinfo is not None and dt.utcoffset() is not None, "Timestamp has no UTC offset")
    return dt.astimezone(timezone.utc)


def object_name(batch_id: str, local_object_path: str) -> str:
    must(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", batch_id) is not None,
         "Invalid batch ID")
    path = Path(local_object_path)
    must(not path.is_absolute() and ".." not in path.parts, "Unsafe batch object path")
    must(re.fullmatch(r"raw/transactions/(customers|products|orders|order_items)/"
                      r"dt=\d{4}-\d{2}-\d{2}/[A-Za-z0-9_\-]+\.jsonl",
                      local_object_path) is not None, "Unexpected raw JSONL object path")
    return f"airflow_batches/{batch_id}/{local_object_path}"


def parse_jsonl(payload: bytes, item: dict, batch_id: str) -> list[dict]:
    kind = item["source_type"]
    must(kind in TABLES, f"Unexpected source type: {kind}")
    source_file = object_name(batch_id, item["object_path"])
    uri = f"gs://{BUCKET}/{source_file}"
    columns = SOURCE_COLUMNS[kind] + META_COLUMNS
    result: list[dict] = []
    for i, line in enumerate(payload.decode("utf-8-sig").splitlines(), start=1):
        must(bool(line.strip()), f"Blank JSONL line: {source_file}:{i}")
        record = json.loads(line, parse_float=Decimal)
        must(isinstance(record, dict), f"JSONL object expected: {source_file}:{i}")
        must(record.get("_source_table") == kind, f"Wrong _source_table: {source_file}:{i}")
        missing = set(columns) - set(record)
        must(not missing, f"JSONL missing columns {sorted(missing)}: {source_file}:{i}")
        extra = set(record) - set(columns)
        must(not extra, f"Unexpected JSONL columns {sorted(extra)}: {source_file}:{i}")
        cleaned = {col: record[col] for col in columns}
        for col in DECIMAL_COLUMNS.intersection(cleaned):
            if cleaned[col] is not None:
                cleaned[col] = str(cleaned[col])  # Preserve BigQuery NUMERIC precision.
        for col in KEY_COLUMNS[kind]:
            if cleaned[col] is not None:
                cleaned[col] = str(cleaned[col])  # All 7 live business key columns are STRING.
        cleaned.update({
            "source_file": source_file,
            "source_line_number": i,
            "gcs_uri": uri,
            "batch_id": batch_id,
        })
        result.append(cleaned)
    must(len(result) == item["record_count"],
         f"JSONL count mismatch: {source_file}: {len(result)} != {item['record_count']}")
    return result


def query_rows(client, sql: str, parameters=None):
    # pyrefly: ignore[missing-import]
    from google.cloud import bigquery
    config = bigquery.QueryJobConfig(query_parameters=parameters or [])
    return list(client.query(sql, job_config=config, location=LOCATION).result())


def one_file_param(source_file: str):
    # pyrefly: ignore[missing-import]
    from google.cloud import bigquery
    return [bigquery.ScalarQueryParameter("file", "STRING", source_file)]


def verify_live_checkpoints(client, checkpoint_path: Path) -> None:
    checkpoint_doc = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    must(checkpoint_doc.get("verified") is True and
         checkpoint_doc.get("source") == "de_etl.watermarks", "Invalid verified checkpoint export")
    local = checkpoint_doc.get("committed_watermarks")
    must(isinstance(local, dict) and set(local) == TABLES, "Incomplete checkpoint export")
    rows = query_rows(client, f"SELECT source_table, committed_watermark FROM `{CHECKPOINT_ID}`")
    must(len(rows) == 4, "Live BigQuery committed watermark table must contain exactly 4 rows")
    seen = set()
    for row in rows:
        kind = row.source_table
        must(kind in TABLES and kind not in seen, "Unexpected/duplicate committed source_table")
        seen.add(kind)
        must(row.committed_watermark is not None, f"NULL live checkpoint for {kind}")
        must(row.committed_watermark.astimezone(timezone.utc) == timestamp_utc(local[kind]),
             f"STALE LOCAL CHECKPOINT for {kind}: refresh/audit before creating another batch")
    must(seen == TABLES, "Missing live checkpoint table")
    print("[OK] Four BigQuery live committed checkpoints match local verified export")


def verify_bigquery_schemas(client, kinds: set[str]) -> dict:
    reg = client.get_table(REGISTRY_ID)
    must(reg.location.lower() == LOCATION, "Registry is in the wrong BigQuery region")
    reg_cols = {f.name: f.field_type.upper() for f in reg.schema}
    required_registry = {"source_file", "source_type", "sha256", "record_count",
                         "gcs_uri", "batch_id", "load_job_id", "loaded_at"}
    must(required_registry <= reg_cols.keys(), "BigQuery registry schema missing columns")
    tables = {}
    for kind in sorted(kinds):
        target_id = f"{PROJECT_ID}.de_raw.{kind}"
        table = client.get_table(target_id)
        must(table.location.lower() == LOCATION, f"Wrong BigQuery region for {target_id}")
        mapping = {f.name: f for f in table.schema}
        expected = SOURCE_COLUMNS[kind] + META_COLUMNS + ENRICH_COLUMNS
        missing = set(expected) - mapping.keys()
        must(not missing, f"BigQuery RAW schema is missing {sorted(missing)} in {target_id}")
        for col in KEY_COLUMNS[kind]:
            must(mapping[col].field_type.upper() == "STRING",
                 f"BigQuery {target_id}.{col} must be STRING (corrected schema)")
        for col in DECIMAL_COLUMNS.intersection(mapping):
            must(mapping[col].field_type.upper() == "NUMERIC",
                 f"BigQuery {target_id}.{col} must be NUMERIC")
        tables[kind] = (target_id, mapping)
    return tables


def registry_state(client, target_id: str, source_file: str):
    rows = query_rows(client,
        f"SELECT source_file, source_type, sha256, record_count, gcs_uri, batch_id "
        f"FROM `{REGISTRY_ID}` WHERE source_file=@file",
        one_file_param(source_file))
    must(len(rows) <= 1, f"Duplicate registry entries for {source_file}")
    raw_rows = query_rows(client,
        f"SELECT COUNT(*) AS n FROM `{target_id}` WHERE source_file=@file",
        one_file_param(source_file))
    must(len(raw_rows) == 1, "Unable to count RAW file records")
    return (rows[0] if rows else None), int(raw_rows[0].n)


def classify_record(reg, raw_count: int, item: dict, source_file: str, batch_id: str) -> str:
    expected_count = item["record_count"]
    if reg is None:
        must(raw_count == 0, f"Orphan RAW rows without registry ({raw_count}): {source_file}")
        return "READY"
    expected_uri = f"gs://{BUCKET}/{source_file}"
    must(reg.source_type == item["source_type"] and
         reg.sha256.lower() == item["sha256"].lower() and
         reg.record_count == expected_count and
         reg.gcs_uri == expected_uri and
         reg.batch_id == batch_id and raw_count == expected_count,
         f"RAW / registry conflict: {source_file}; manual audit required")
    return "SKIP_VERIFIED"


def verify_gcs(bucket, manifest_path: Path, manifest: dict) -> list[tuple[dict, list[dict]]]:
    batch_id = manifest["batch_id"]
    prefix = f"airflow_batches/{batch_id}/"
    expected_names = {prefix + "manifest.json"}
    for item in manifest["files"]:
        expected_names.add(object_name(batch_id, item["object_path"]))
    observed_names = {blob.name for blob in bucket.list_blobs(prefix=prefix)}
    must(observed_names == expected_names,
         f"GCS batch mismatch: missing={sorted(expected_names-observed_names)}, "
         f"unexpected={sorted(observed_names-expected_names)}")
    local_manifest = manifest_path.read_bytes()
    remote_manifest = bucket.blob(prefix + "manifest.json").download_as_bytes()
    must(local_manifest == remote_manifest, "Remote manifest differs from immutable local manifest")
    print("[OK] Remote GCS manifest matches local manifest exactly")
    payloads = []
    for item in manifest["files"]:
        name = object_name(batch_id, item["object_path"])
        data = bucket.blob(name).download_as_bytes()
        must(len(data) == item["size_bytes"] and sha256(data) == item["sha256"].lower(),
             f"GCS JSONL size/hash mismatch: {name}")
        parsed = parse_jsonl(data, item, batch_id)
        payloads.append((item, parsed))
        print(f"[OK] Remote JSONL verified: {name} ({len(parsed)} rows)")
    return payloads


def apply_one_file(client, bigquery, item: dict, rows: list[dict], table_details: tuple, batch_id: str):
    kind = item["source_type"]
    source_file = object_name(batch_id, item["object_path"])
    target_id, schema_map = table_details
    reg, raw_count = registry_state(client, target_id, source_file)
    status = classify_record(reg, raw_count, item, source_file, batch_id)
    if status == "SKIP_VERIFIED":
        print(f"[SKIP_VERIFIED] {source_file}: {raw_count} rows; no writes")
        return "skipped"

    # Stage in a separate table; never load directly into RAW without registry.
    stage_id = f"{PROJECT_ID}.de_etl._stage_{kind}_{uuid.uuid4().hex[:16]}"
    cols = SOURCE_COLUMNS[kind] + META_COLUMNS + ENRICH_COLUMNS
    load_job = None
    try:
        load_job = client.load_table_from_json(
            rows, stage_id,
            job_config=bigquery.LoadJobConfig(
                schema=[schema_map[col] for col in cols],
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                ignore_unknown_values=False,
                max_bad_records=0,
            ),
            location=LOCATION,
        )
        load_job.result()
        stage_count = query_rows(client, f"SELECT COUNT(*) AS n FROM `{stage_id}`")[0].n
        must(stage_count == item["record_count"],
             f"Staging record count mismatch for {source_file}")

        params = [
            bigquery.ScalarQueryParameter("file", "STRING", source_file),
            bigquery.ScalarQueryParameter("kind", "STRING", kind),
            bigquery.ScalarQueryParameter("sha256", "STRING", item["sha256"].lower()),
            bigquery.ScalarQueryParameter("n", "INT64", item["record_count"]),
            bigquery.ScalarQueryParameter("uri", "STRING", f"gs://{BUCKET}/{source_file}"),
            bigquery.ScalarQueryParameter("batch", "STRING", batch_id),
            bigquery.ScalarQueryParameter("job", "STRING", load_job.job_id),
        ]
        selected = ", ".join(f"`{col}`" for col in cols)
        sql = f"""
        BEGIN TRANSACTION;
        -- BigQuery recommends a no-op DELETE to acquire a mutation lock and
        -- reduce races between insert-only concurrent transactions.
        DELETE FROM `{REGISTRY_ID}` WHERE FALSE;
        INSERT INTO `{target_id}` ({selected})
        SELECT {selected} FROM `{stage_id}`
        WHERE NOT EXISTS (SELECT 1 FROM `{REGISTRY_ID}` WHERE source_file=@file);
        INSERT INTO `{REGISTRY_ID}`
          (source_file, source_type, sha256, record_count, gcs_uri, batch_id, load_job_id)
        SELECT @file, @kind, @sha256, @n, @uri, @batch, @job
        FROM (SELECT 1) AS seed
        WHERE NOT EXISTS (SELECT 1 FROM `{REGISTRY_ID}` WHERE source_file=@file);
        COMMIT TRANSACTION;
        """
        query_rows(client, sql, params)
        after, after_raw_count = registry_state(client, target_id, source_file)
        must(classify_record(after, after_raw_count, item, source_file, batch_id)
             == "SKIP_VERIFIED", f"Post-commit verification failed: {source_file}")
        print(f"[LOADED] {source_file}: {after_raw_count} RAW rows + 1 registry entry")
        return "loaded"
    finally:
        try:
            client.delete_table(stage_id, not_found_ok=True)
        except Exception as exc:
            print(f"[WARNING] Temporary staging cleanup failed: {stage_id} ({exc})")


def process(manifest_path: Path, checkpoint_path: Path, apply: bool) -> None:
    src_dir = Path(__file__).resolve().parents[1] / "ingestion"
    sys.path.insert(0, str(src_dir))
    # pyrefly: ignore[missing-import]
    from validate_batch_manifest import validate

    result = validate(manifest_path, checkpoint_path)
    manifest_path = manifest_path.resolve()
    batch_id = result["batch_id"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    must(result["records"] > 0 and result["files"] > 0, "EMPTY BATCH: skip BigQuery RAW loader")

    # pyrefly: ignore[missing-import]
    from google.cloud import bigquery, storage
    bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    gcs = storage.Client(project=PROJECT_ID)
    verify_live_checkpoints(bq, checkpoint_path)
    table_details = verify_bigquery_schemas(bq, {i["source_type"] for i in manifest["files"]})
    prepared = verify_gcs(gcs.bucket(BUCKET), manifest_path, manifest)

    status_counts = {"READY": 0, "SKIP_VERIFIED": 0}
    for item, _ in prepared:
        name = object_name(batch_id, item["object_path"])
        target_id, _ = table_details[item["source_type"]]
        registry, raw_count = registry_state(bq, target_id, name)
        status = classify_record(registry, raw_count, item, name, batch_id)
        status_counts[status] += 1
        print(f"[{status}] {name}; existing_RAW_rows={raw_count}")

    if not apply:
        print(f"BIGQUERY BATCH CHECK PASSED: batch={batch_id}; "
              f"ready={status_counts['READY']}; skipped={status_counts['SKIP_VERIFIED']}")
        print("READ-ONLY: No RAW, registry, pipeline_batches, watermarks, GCS, or dbt changes")
        return

    print(f"BIGQUERY BATCH APPLY START: batch={batch_id}; files={len(prepared)}")
    loaded = skipped = 0
    for item, rows in prepared:
        result = apply_one_file(bq, bigquery, item, rows,
                                table_details[item["source_type"]], batch_id)
        loaded += result == "loaded"
        skipped += result == "skipped"
    print(f"BIGQUERY BATCH APPLY PASS: batch={batch_id}; "
          f"loaded_files={loaded}; skipped_files={skipped}")
    print("NO WATERMARK COMMIT. Next step: dbt QA + downstream reconciliation.")


def self_test() -> None:
    sample = {
        "customer_id": "C990", "customer_name": "Airflow Test Customer",
        "city": "Hanoi", "created_at": "2026-11-04T10:00:00+00:00",
        "updated_at": "2026-11-04T10:00:00+00:00",
        "_source_table": "customers", "_extracted_at": "2026-11-04T10:01:00+00:00",
        "_watermark_from": "2026-10-19T13:33:45+00:00",
        "_watermark_to": "2026-11-04T10:01:00+00:00",
    }
    item = {"source_type": "customers", "record_count": 1,
            "object_path": "raw/transactions/customers/dt=2026-11-04/customers_test123.jsonl"}
    rows = parse_jsonl((json.dumps(sample) + "\n").encode(), item, "test123")
    assert rows[0]["source_file"] == (
        "airflow_batches/test123/raw/transactions/customers/dt=2026-11-04/"
        "customers_test123.jsonl"
    )
    assert rows[0]["customer_id"] == "C990"
    assert rows[0]["source_line_number"] == 1
    source_file = object_name("test123", item["object_path"])
    reg = SimpleNamespace(source_type="customers", sha256="a" * 64, record_count=1,
                          gcs_uri=f"gs://{BUCKET}/{source_file}", batch_id="test123")
    sample_item = {**item, "sha256": "a" * 64}
    assert classify_record(None, 0, sample_item, source_file, "test123") == "READY"
    assert classify_record(reg, 1, sample_item, source_file, "test123") == "SKIP_VERIFIED"
    try:
        classify_record(None, 1, sample_item, source_file, "test123")
    except RuntimeError:
        pass
    else:
        raise AssertionError("RAW orphan not detected")
    try:
        object_name("test123", "../bad.jsonl")
    except RuntimeError:
        pass
    else:
        raise AssertionError("Unsafe source path accepted")
    assert sha256(b"test") == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    print("BIGQUERY BATCH LOADER SELF-TEST PASS (no network, no writes)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--checkpoints-json", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.manifest or not args.checkpoints_json:
        parser.error("--check/--apply require --manifest and --checkpoints-json")
    process(args.manifest, args.checkpoints_json, args.apply)


if __name__ == "__main__":
    main()
