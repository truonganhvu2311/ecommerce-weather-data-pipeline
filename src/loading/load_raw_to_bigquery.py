"""GCS RAW -> BigQuery RAW (initial verified migration; one file per transaction).

Usage:
  python src/loading/load_raw_to_bigquery.py --check
  python src/loading/load_raw_to_bigquery.py --apply

Designed for the 8 transaction JSONL + 3 weather JSON snapshot. The manifest
is authoritative. Never modifies source PostgreSQL or state/watermark.json.
For a later Airflow DAG, supply a new immutable manifest and a replay-safe
checkpoint protocol; don't reuse this initial migration unmodified as CDC.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
from decimal import Decimal
from pathlib import PurePosixPath

# pyrefly: ignore[missing-import]
from google.cloud import bigquery, storage

PROJECT_ID = "weather-data-project-512207"
BUCKET = "weather-data-project-512207-raw"
LOCATION = "asia-southeast1"
MANIFEST_KEY = "manifests/gcs_migration_manifest.csv"
DEFAULT_BATCH_ID = "initial_gcs_migration_2026_11_01"

SOURCE_COLUMNS = {
    "customers": ["customer_id", "customer_name", "city", "created_at", "updated_at"],
    "products": ["product_id", "product_name", "category", "price", "created_at", "updated_at"],
    "orders": ["order_id", "customer_id", "order_date", "status", "city",
               "total_amount", "created_at", "updated_at", "is_deleted"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity",
                    "unit_price", "created_at", "updated_at"],
}
META_COLUMNS = ["_source_table", "_extracted_at", "_watermark_from", "_watermark_to"]
DECIMAL_COLUMNS = {"price", "total_amount", "unit_price"}
ENRICHED_COLUMNS = ["source_file", "source_line_number", "gcs_uri", "batch_id"]
WEATHER_COLUMNS = ["source_file", "payload", "gcs_uri", "batch_id"]
VALID_PATH = re.compile(r"^raw/transactions/(customers|products|orders|order_items)/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/[^/]+\.jsonl$")


def load_manifest(bucket):
    blob = bucket.blob(MANIFEST_KEY)
    if not blob.exists():
        raise RuntimeError(f"Missing GCS manifest: gs://{BUCKET}/{MANIFEST_KEY}")
    text = blob.download_as_bytes().decode("utf-8-sig")
    items = list(csv.DictReader(io.StringIO(text)))
    if len(items) != 11:
        raise ValueError(f"Expected 11 manifest entries; got {len(items)}")
    seen = set()
    types = {"customers": 0, "products": 0, "orders": 0, "order_items": 0, "weather": 0}
    for item in items:
        key = item["object_path"]
        if key in seen:
            raise ValueError(f"Duplicate manifest object_path: {key}")
        seen.add(key)
        if VALID_PATH.fullmatch(key):
            kind = key.split("/")[2]
        elif key.startswith("raw/weather/") and key.endswith(".json") and key.count("/") == 2:
            kind = "weather"
        else:
            raise ValueError(f"Unexpected manifest object_path: {key}")
        if not re.fullmatch(r"[a-fA-F0-9]{64}", item["sha256"]):
            raise ValueError(f"Invalid sha256: {key}")
        if int(item["size_bytes"]) <= 0:
            raise ValueError(f"Invalid file size: {key}")
        item["kind"] = kind
        types[kind] += 1
    if types != {"customers": 1, "products": 1, "orders": 4, "order_items": 2, "weather": 3}:
        raise ValueError(f"Unexpected manifest type counts: {types}")
    # Fail closed if another RAW document was accidentally uploaded.
    actual = {
        blob.name for blob in bucket.list_blobs(prefix="raw/")
        if blob.name.endswith((".jsonl", ".json"))
    }
    if actual != seen:
        raise RuntimeError(f"GCS/manifest mismatch. Missing: {sorted(seen - actual)}; extra: {sorted(actual - seen)}")
    return sorted(items, key=lambda r: r["object_path"])


def parse_file(item, data, batch_id):
    key, kind = item["object_path"], item["kind"]
    uri = f"gs://{BUCKET}/{key}"
    if kind == "weather":
        value = json.loads(data.decode("utf-8"))
        if not isinstance(value, dict) or not {"metadata", "response"}.issubset(value):
            raise ValueError(f"Invalid weather JSON: {key}")
        return [{"source_file": PurePosixPath(key).name, "payload": value,
                 "gcs_uri": uri, "batch_id": batch_id}]

    columns = SOURCE_COLUMNS[kind] + META_COLUMNS
    parsed = []
    lines = data.decode("utf-8-sig").splitlines()
    if not lines:
        raise ValueError(f"Empty JSONL file: {key}")
    for line_no, line in enumerate(lines, start=1):
        if not line.strip():
            raise ValueError(f"Empty JSONL line {line_no}: {key}")
        record = json.loads(line, parse_float=Decimal)
        if not isinstance(record, dict) or record.get("_source_table") != kind:
            raise ValueError(f"Invalid _source_table at line {line_no}: {key}")
        missing = set(columns) - record.keys()
        if missing:
            raise ValueError(f"Missing keys {sorted(missing)} at line {line_no}: {key}")
        cleaned = {}
        for col in columns:
            value = record[col]
            # BigQuery's JSON loader accepts decimal strings for NUMERIC.
            if col in DECIMAL_COLUMNS and value is not None:
                value = str(value)
            cleaned[col] = value
        cleaned.update({"source_file": key, "source_line_number": line_no,
                        "gcs_uri": uri, "batch_id": batch_id})
        parsed.append(cleaned)
    return parsed


def ensure_target_schemas(client):
    required = {
        **{f"de_raw.{name}": set(cols + META_COLUMNS + ENRICHED_COLUMNS)
           for name, cols in SOURCE_COLUMNS.items()},
        "de_raw.weather_api_responses": set(WEATHER_COLUMNS),
        "de_etl.loaded_files": {"source_file", "source_type", "sha256", "record_count",
                                "gcs_uri", "batch_id", "load_job_id", "loaded_at"},
    }
    for suffix, fields in required.items():
        table = client.get_table(f"{PROJECT_ID}.{suffix}")
        missing = fields - {field.name for field in table.schema}
        if missing:
            raise RuntimeError(f"Wrong schema for {suffix}, missing: {sorted(missing)}")
        if table.location.lower() != LOCATION.lower():
            raise RuntimeError(f"Wrong BigQuery location: {suffix}: {table.location}")


def query_scalar(client, sql, params):
    config = bigquery.QueryJobConfig(query_parameters=params)
    return list(client.query(sql, job_config=config, location=LOCATION).result())[0][0]


def registry_record(client, source_file):
    sql = f"SELECT sha256, record_count FROM `{PROJECT_ID}.de_etl.loaded_files` WHERE source_file=@file"
    params = [bigquery.ScalarQueryParameter("file", "STRING", source_file)]
    rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=LOCATION).result())
    if len(rows) > 1:
        raise RuntimeError(f"Duplicate file registry rows: {source_file}")
    return rows[0] if rows else None


def count_raw(client, table_id, source_file):
    sql = f"SELECT COUNT(*) FROM `{table_id}` WHERE source_file=@file"
    return query_scalar(client, sql, [bigquery.ScalarQueryParameter("file", "STRING", source_file)])


def insert_file(client, item, rows, batch_id):
    kind = item["kind"]
    key = item["object_path"]
    source_file = PurePosixPath(key).name if kind == "weather" else key
    target_name = "weather_api_responses" if kind == "weather" else kind
    target_id = f"{PROJECT_ID}.de_raw.{target_name}"
    expected_hash = item["sha256"].lower()
    expected_count = len(rows)
    existing = registry_record(client, source_file)
    raw_count = count_raw(client, target_id, source_file)
    if existing:
        if existing.sha256.strip().lower() != expected_hash or existing.record_count != expected_count:
            raise RuntimeError(f"Registered file has changed: {source_file}")
        if raw_count != expected_count:
            raise RuntimeError(f"Registry and RAW counts disagree: {source_file}: {raw_count} != {expected_count}")
        print(f"[SKIP] Already ingested: {source_file} ({expected_count} rows)")
        return "skipped", 0
    if raw_count:
        raise RuntimeError(f"Found {raw_count} RAW rows without registry for {source_file}; manual audit required")

    columns = WEATHER_COLUMNS if kind == "weather" else SOURCE_COLUMNS[kind] + META_COLUMNS + ENRICHED_COLUMNS
    target = client.get_table(target_id)
    mapping = {field.name: field for field in target.schema}
    stage_schema = [mapping[name] for name in columns]
    stage_table = f"{PROJECT_ID}.de_etl._stage_{target_name}_{expected_hash[:20]}"

    job_config = bigquery.LoadJobConfig(
        schema=stage_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=False,
        max_bad_records=0,
    )
    load_job = client.load_table_from_json(
        rows, stage_table, job_config=job_config, location=LOCATION
    )
    load_job.result()
    if client.get_table(stage_table).num_rows != expected_count:
        raise RuntimeError(f"Stage count mismatch for {source_file}")

    field_names = ", ".join(f"`{col}`" for col in columns)
    params = [
        bigquery.ScalarQueryParameter("file", "STRING", source_file),
        bigquery.ScalarQueryParameter("sha256", "STRING", expected_hash),
        bigquery.ScalarQueryParameter("kind", "STRING", kind),
        bigquery.ScalarQueryParameter("n", "INT64", expected_count),
        bigquery.ScalarQueryParameter("gcs_uri", "STRING", item["gcs_uri"]),
        bigquery.ScalarQueryParameter("batch", "STRING", batch_id),
        bigquery.ScalarQueryParameter("load_job_id", "STRING", load_job.job_id),
    ]
    # Both RAW insert and loaded_files registry insert commit atomically.
    # The WHERE NOT EXISTS checks offer replay guards; run one loader at a time.
    sql = f"""
    BEGIN TRANSACTION;
    INSERT INTO `{target_id}` ({field_names})
    SELECT {field_names} FROM `{stage_table}`
    WHERE NOT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.de_etl.loaded_files` WHERE source_file=@file
    );
    INSERT INTO `{PROJECT_ID}.de_etl.loaded_files`
      (source_file, source_type, sha256, record_count, gcs_uri, batch_id, load_job_id)

    SELECT @file, @kind, @sha256, @n, @gcs_uri, @batch, @load_job_id
    FROM (SELECT 1) AS registry_input
    WHERE NOT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.de_etl.loaded_files` WHERE source_file=@file
    );

    COMMIT TRANSACTION;
    """
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=LOCATION).result()
    after = registry_record(client, source_file)
    count = count_raw(client, target_id, source_file)
    if not after or after.sha256.strip().lower() != expected_hash or count != expected_count:
        raise RuntimeError(f"Post-commit verification FAILED: {source_file}")
    try:
        client.delete_table(stage_table, not_found_ok=True)
    except Exception as exc:
        # The RAW + registry transaction is already committed.
        print(f"[WARNING] Stage cleanup failed for {source_file}: {exc}")
    print(f"[LOADED] {source_file}: {count} rows")
    return "loaded", count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="GCS/manifest/schema check; no writes")
    mode.add_argument("--apply", action="store_true", help="Load the verified snapshot to BigQuery")
    parser.add_argument("--batch-id", default=DEFAULT_BATCH_ID)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_\-]{1,128}", args.batch_id):
        raise ValueError("batch_id must be 1-128 letters, numbers, underscore or dash")

    sclient = storage.Client(project=PROJECT_ID)
    bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    bucket = sclient.bucket(BUCKET)
    manifest = load_manifest(bucket)
    ensure_target_schemas(bq)
    print(f"Manifest: {len(manifest)} files; bucket gs://{BUCKET}; BigQuery location: {LOCATION}")
    # Verify ALL source files before performing any BigQuery mutation.
    prepared = []
    for item in manifest:
        key = item["object_path"]
        payload = bucket.blob(key).download_as_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != item["sha256"].lower() or len(payload) != int(item["size_bytes"]):
            raise ValueError(f"SHA256 or size MISMATCH: {key}")
        item["gcs_uri"] = f"gs://{BUCKET}/{key}"
        rows = parse_file(item, payload, args.batch_id)
        prepared.append((item, rows))
        print(f"[VERIFIED] {key}: {len(rows)} records, SHA256 matched")

    loaded = skipped = new_rows = 0
    if args.apply:
        for item, rows in prepared:
            result, number = insert_file(bq, item, rows, args.batch_id)
            loaded += result == "loaded"
            skipped += result == "skipped"
            new_rows += number
    if args.check:
        print("CHECK PASSED: 11/11 files + RAW schemas. No BigQuery rows were added.")
    else:
        print(f"SUMMARY: loaded_files={loaded}, skipped_files={skipped}, new_rows={new_rows}")
        print("Do NOT update state/watermark.json: initial migration only.")


if __name__ == "__main__":
    main()
