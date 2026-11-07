"""Day 8.7.10.9: manually finalize a QA-verified incremental batch.

Run modes:
  --self-test: no external dependencies, no writes
  --check --manifest PATH --checkpoints-json PATH: read-only preflight
  --apply --manifest PATH --checkpoints-json PATH --qa-confirmed: atomic BQ metadata commit
  --export --manifest PATH --output PATH: exclusive-create a VERIFIED next checkpoint JSON

Never changes RAW, GCS, dbt, or the immutable pending manifest. Does not overwrite
legacy state/watermark.json or the old committed checkpoint export.
IMPORTANT: --qa-confirmed is a manual assertion that the operator checked a NEW
Airflow dbt build (87/87) after RAW load, and downstream reconciliation PASS.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ID = "weather-data-project-512207"
BUCKET = "weather-data-project-512207-raw"
LOCATION = "asia-southeast1"
TABLES = ("customers", "products", "orders", "order_items")
WATERMARKS_ID = f"{PROJECT_ID}.de_etl.watermarks"
BATCHES_ID = f"{PROJECT_ID}.de_etl.pipeline_batches"
REGISTRY_ID = f"{PROJECT_ID}.de_etl.loaded_files"
BATCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
ROOT = Path(__file__).resolve().parents[2]
LEGACY = ROOT / "state" / "watermark.json"


def must(ok: bool, message: str) -> None:
    if not ok:
        raise RuntimeError(message)


def utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        must(isinstance(value, str), "Invalid timestamp type")
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    must(dt.tzinfo is not None and dt.utcoffset() is not None,
         "Timestamp is missing timezone information")
    return dt.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return utc(value).isoformat()


def load_manifest(path: Path) -> dict:
    must(path.is_file(), f"Manifest does not exist: {path}")
    m = json.loads(path.read_text(encoding="utf-8"))
    must(isinstance(m, dict), "Manifest must be an object")
    batch = m.get("batch_id")
    must(isinstance(batch, str) and BATCH_RE.fullmatch(batch) is not None,
         "Invalid batch_id")
    must(path.parent.name == batch and m.get("schema_version") == 1,
         "Manifest path/schema does not match batch")
    must(m.get("status") == "pending" and m.get("source") == "postgresql_timestamp_incremental",
         "Expected immutable pending PostgreSQL batch")
    must(all(isinstance(m.get(k), dict) and set(m[k]) == set(TABLES)
             for k in ("watermark_from", "watermark_to", "row_counts")),
         "Four source tables required in all watermark/count maps")
    for kind in TABLES:
        must(utc(m["watermark_from"][kind]) < utc(m["watermark_to"][kind]),
             f"Invalid watermark bounds for {kind}")
        must(type(m["row_counts"][kind]) is int and m["row_counts"][kind] >= 0,
             f"Invalid count for {kind}")
    files = m.get("files")
    must(isinstance(files, list) and 1 <= len(files) <= 4,
         "This finalizer requires 1-4 nonempty source files")
    seen = set()
    for item in files:
        must(isinstance(item, dict), "Invalid file entry")
        kind, rel = item.get("source_type"), item.get("local_path")
        must(kind in TABLES and kind not in seen, "Unknown/duplicate source type")
        seen.add(kind)
        must(isinstance(rel, str) and rel == item.get("object_path"),
             "local_path and object_path must match")
        pattern = (rf"raw/transactions/{kind}/dt=\d{{4}}-\d{{2}}-\d{{2}}/"
                   rf"{kind}_{re.escape(batch)}\.jsonl")
        must(re.fullmatch(pattern, rel) is not None, "Unexpected source path")
        p = (path.parent / rel).resolve()
        must(p.is_relative_to(path.parent.resolve()) and p.is_file(),
             f"Missing or unsafe JSONL: {rel}")
        payload = p.read_bytes()
        must(len(payload) == item.get("size_bytes") and
             hashlib.sha256(payload).hexdigest() == item.get("sha256"),
             f"Local file differs from manifest: {rel}")
        lines = payload.decode("utf-8-sig").splitlines()
        must(type(item.get("record_count")) is int and item["record_count"] > 0 and
             len(lines) == item["record_count"] and all(line.strip() for line in lines),
             f"Bad JSONL row count: {rel}")
        for line in lines:
            rec = json.loads(line)
            must(isinstance(rec, dict) and rec.get("_source_table") == kind and
                 utc(rec["_watermark_from"]) == utc(m["watermark_from"][kind]) and
                 utc(rec["_watermark_to"]) == utc(m["watermark_to"][kind]) and
                 utc(m["watermark_from"][kind]) < utc(rec["updated_at"]) <=
                    utc(m["watermark_to"][kind]),
                 f"Row metadata outside batch bounds: {rel}")
    actual = {kind: next((item["record_count"] for item in files
                          if item["source_type"] == kind), 0) for kind in TABLES}
    must(actual == m["row_counts"], "JSONL row counts do not match manifest")
    return m


def checkpoint_file(path: Path) -> dict[str, datetime]:
    must(path.is_file(), f"Checkpoint file is missing: {path}")
    doc = json.loads(path.read_text(encoding="utf-8"))
    must(doc.get("source") == "de_etl.watermarks" and doc.get("verified") is True and
         doc.get("project_id") == PROJECT_ID,
         "Invalid / non-verified checkpoint export")
    mapping = doc.get("committed_watermarks")
    must(isinstance(mapping, dict) and set(mapping) == set(TABLES),
         "Require exactly four exported checkpoints")
    return {t: utc(mapping[t]) for t in TABLES}


def bigquery_rows(client, sql: str, parameters=None):
    from google.cloud import bigquery
    config = bigquery.QueryJobConfig(query_parameters=parameters or [])
    return list(client.query(sql, job_config=config, location=LOCATION).result())


def bq_params(*items):
    from google.cloud import bigquery
    return [bigquery.ScalarQueryParameter(name, kind, value) for name, kind, value in items]


def live_watermarks(client) -> tuple[dict[str, datetime], dict[str, str]]:
    rows = bigquery_rows(client,
        f"SELECT source_table, committed_watermark, committed_batch_id "
        f"FROM `{WATERMARKS_ID}`")
    must(len(rows) == 4, f"Expected 4 committed watermark rows, got {len(rows)}")
    seen = set()
    values, batches = {}, {}
    for r in rows:
        must(r.source_table in TABLES and r.source_table not in seen and
             r.committed_watermark is not None,
             f"Invalid/duplicate live watermark: {r.source_table}")
        seen.add(r.source_table)
        values[r.source_table] = utc(r.committed_watermark)
        batches[r.source_table] = r.committed_batch_id
    must(seen == set(TABLES), "Missing source watermark")
    return values, batches


def batch_rows(client, batch_id: str):
    rows = bigquery_rows(client,
        f"SELECT batch_id, status, manifest_uri, "
        f"TO_JSON_STRING(watermark_from) AS from_json, "
        f"TO_JSON_STRING(watermark_to) AS to_json "
        f"FROM `{BATCHES_ID}` WHERE batch_id=@batch",
        bq_params(("batch", "STRING", batch_id)))
    must(len(rows) <= 1, "Duplicate pipeline_batches rows - manual audit required")
    return rows[0] if rows else None


def maps_match(a: dict, b: dict) -> bool:
    return isinstance(a, dict) and set(a) == set(b) and all(
        utc(a[t]) == utc(b[t]) for t in b)


def verify_batch_meta(row, manifest: dict, *, require_committed: bool) -> None:
    if row is None:
        must(not require_committed, "Missing committed pipeline_batches entry")
        return
    bid = manifest["batch_id"]
    uri = f"gs://{BUCKET}/airflow_batches/{bid}/manifest.json"
    must(row.manifest_uri in (None, uri), "pipeline_batches manifest URI mismatch")
    for key, serialized in (("watermark_from", row.from_json),
                            ("watermark_to", row.to_json)):
        if serialized is not None:
            must(maps_match(json.loads(serialized), manifest[key]),
                 f"pipeline_batches {key} mismatch")
    if require_committed:
        must(row.status == "committed", "Batch status is not committed")
    else:
        must(row.status in ("pending", "uploaded", "loaded", "validated", "committed"),
             "Unexpected pipeline_batches status")


def verify_gcs(manifest_path: Path, manifest: dict, gcs_client) -> None:
    bucket = gcs_client.bucket(BUCKET)
    prefix = f"airflow_batches/{manifest['batch_id']}/"
    names = {prefix + "manifest.json"} | {prefix + x["object_path"] for x in manifest["files"]}
    actual = {blob.name for blob in bucket.list_blobs(prefix=prefix)}
    must(actual == names, f"GCS object list mismatch. Missing={names-actual}; Extra={actual-names}")
    must(bucket.blob(prefix + "manifest.json").download_as_bytes() == manifest_path.read_bytes(),
         "GCS manifest does not match immutable local manifest")
    for item in manifest["files"]:
        payload = bucket.blob(prefix + item["object_path"]).download_as_bytes()
        must(len(payload) == item["size_bytes"] and
             hashlib.sha256(payload).hexdigest() == item["sha256"],
             f"GCS file content checksum mismatch: {item['object_path']}")
    print("[OK] GCS manifest and JSONL hashes match immutable local files")


def verify_raw_registry(client, manifest: dict) -> None:
    batch = manifest["batch_id"]
    prefix = f"airflow_batches/{batch}/"
    regs = bigquery_rows(client,
        f"SELECT source_file, source_type, sha256, record_count, gcs_uri, batch_id "
        f"FROM `{REGISTRY_ID}` WHERE batch_id=@batch",
        bq_params(("batch", "STRING", batch)))
    expected = {prefix + it["object_path"]: it for it in manifest["files"]}
    must(len(regs) == len(expected), "Registry count mismatch for batch")
    found = set()
    for reg in regs:
        name = reg.source_file
        must(name in expected and name not in found, "Unexpected/duplicate registry file")
        found.add(name)
        item = expected[name]
        must(reg.source_type == item["source_type"] and
             reg.sha256 == item["sha256"] and
             reg.record_count == item["record_count"] and
             reg.gcs_uri == f"gs://{BUCKET}/{name}",
             f"Registry metadata mismatch: {name}")
        kind = item["source_type"]
        stats = bigquery_rows(client,
            f"SELECT COUNT(*) AS n, "
            f"COUNTIF(_watermark_from = @from AND _watermark_to = @to) AS matched "
            f"FROM `{PROJECT_ID}.de_raw.{kind}` "
            f"WHERE source_file=@file AND batch_id=@batch",
            bq_params(("from", "TIMESTAMP", utc(manifest["watermark_from"][kind])),
                      ("to", "TIMESTAMP", utc(manifest["watermark_to"][kind])),
                      ("file", "STRING", name), ("batch", "STRING", batch)))
        must(len(stats) == 1 and stats[0].n == item["record_count"] and
             stats[0].matched == item["record_count"],
             f"RAW file count/bounds mismatch: {name}")
    print(f"[OK] RAW & registry reconciled: {len(expected)} files, "
          f"{sum(it['record_count'] for it in manifest['files'])} rows")


def verify_customer_c990(client, manifest: dict) -> None:
    if manifest["batch_id"] != "day8_customer_c990_20261104":
        return  # This demonstration-specific downstream check only applies to C990.
    rows = bigquery_rows(client,
        f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.de_staging.stg_customers` "
        "WHERE customer_id='C990'")
    must(len(rows) == 1 and rows[0].n == 1,
         "C990 is missing or duplicated in staging; rerun dbt QA")
    print("[OK] C990 appears once in stg_customers")


def determine_state(live: dict, batches: dict, manifest: dict, row) -> str:
    base = {t: utc(manifest["watermark_from"][t]) for t in TABLES}
    target = {t: utc(manifest["watermark_to"][t]) for t in TABLES}
    if all(live[t] == target[t] and batches[t] == manifest["batch_id"] for t in TABLES):
        verify_batch_meta(row, manifest, require_committed=True)
        return "COMMITTED"
    must(all(live[t] == base[t] for t in TABLES),
         "Live checkpoints diverged; do not finalize or overwrite checkpoint export")
    must(row is None or row.status != "committed",
         "Inconsistent state: batch committed but checkpoints are old")
    verify_batch_meta(row, manifest, require_committed=False)
    return "READY"


def transaction_sql() -> str:
    whens = "\n".join(f"  WHEN '{t}' THEN @to_{t}" for t in TABLES)
    conditions = " OR\n".join(
        f"(w.source_table='{t}' AND w.committed_watermark=@from_{t})" for t in TABLES)
    return f"""
DECLARE affected INT64 DEFAULT 0;
BEGIN
  BEGIN TRANSACTION;
  UPDATE `{WATERMARKS_ID}` AS w
  SET committed_watermark = CASE w.source_table\n{whens}\n    ELSE w.committed_watermark END,
      committed_batch_id = @batch,
      committed_at = CURRENT_TIMESTAMP()
  WHERE {conditions};
  SET affected = @@row_count;
  IF affected != 4 THEN
    RAISE USING MESSAGE = 'Watermark concurrency/precondition mismatch: expected 4 rows';
  END IF;
  MERGE `{BATCHES_ID}` AS target
  USING (SELECT @batch AS batch_id, @manifest_uri AS manifest_uri,
                PARSE_JSON(@from_json) AS wm_from, PARSE_JSON(@to_json) AS wm_to) AS src
  ON target.batch_id=src.batch_id
  WHEN MATCHED AND target.status IN ('pending','uploaded','loaded','validated') THEN
    UPDATE SET status='committed', manifest_uri=src.manifest_uri,
               watermark_from=src.wm_from, watermark_to=src.wm_to,
               updated_at=CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (batch_id, status, manifest_uri, watermark_from, watermark_to, updated_at)
    VALUES (src.batch_id, 'committed', src.manifest_uri, src.wm_from, src.wm_to, CURRENT_TIMESTAMP());
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
"""


def live_preflight(client, manifest_path: Path, manifest: dict, checkpoint_path: Path | None,
                   *, allow_ready: bool) -> tuple[str, dict]:
    from google.cloud import storage
    verify_gcs(manifest_path, manifest, storage.Client(project=PROJECT_ID))
    verify_raw_registry(client, manifest)
    verify_customer_c990(client, manifest)
    live, last_batch_ids = live_watermarks(client)
    previous = batch_rows(client, manifest["batch_id"])
    state = determine_state(live, last_batch_ids, manifest, previous)
    if not allow_ready:
        must(state == "COMMITTED", "Batch not committed; export is not allowed")
    if checkpoint_path is not None:
        local = checkpoint_file(checkpoint_path)
        expected = manifest["watermark_from"] if state == "READY" else manifest["watermark_to"]
        must(all(local[t] == utc(expected[t]) for t in TABLES),
             "Verified local checkpoint is stale/incorrect for current batch state")
    print(f"[OK] Batch state: {state}; four live checkpoint rows verified")
    return state, live


def execute_apply(client, manifest: dict) -> None:
    from google.cloud import bigquery
    batch = manifest["batch_id"]
    params = [bigquery.ScalarQueryParameter("batch", "STRING", batch),
              bigquery.ScalarQueryParameter("manifest_uri", "STRING",
                f"gs://{BUCKET}/airflow_batches/{batch}/manifest.json"),
              bigquery.ScalarQueryParameter("from_json", "STRING",
                json.dumps(manifest["watermark_from"], sort_keys=True)),
              bigquery.ScalarQueryParameter("to_json", "STRING",
                json.dumps(manifest["watermark_to"], sort_keys=True))]
    for t in TABLES:
        params.extend((
            bigquery.ScalarQueryParameter(f"from_{t}", "TIMESTAMP", utc(manifest["watermark_from"][t])),
            bigquery.ScalarQueryParameter(f"to_{t}", "TIMESTAMP", utc(manifest["watermark_to"][t])),
        ))
    bigquery_rows(client, transaction_sql(), params)


def run(args) -> None:
    from google.cloud import bigquery
    manifest_path = args.manifest.resolve()
    m = load_manifest(manifest_path)
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    if args.export:
        state, values = live_preflight(client, manifest_path, m, None, allow_ready=False)
        output = args.output.resolve()
        must(output != LEGACY.resolve() and output != manifest_path and not output.exists(),
             "Refusing overwrite of existing export/legacy watermark/manifest")
        must(output.name != "committed_watermarks_verified.json",
             "Export to a NEW filename, then replace the old checkpoint only after backup")
        doc = {
            "source": "de_etl.watermarks", "verified": True, "project_id": PROJECT_ID,
            "committed_batch_id": m["batch_id"],
            "exported_at_utc": iso(datetime.now(timezone.utc)),
            "committed_watermarks": {t: iso(values[t]) for t in TABLES},
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(doc, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
        print(f"CHECKPOINT EXPORT PASS: {output}")
        print("BigQuery, RAW, GCS, and original verified checkpoint file unchanged")
        return

    must(args.checkpoints_json is not None,
         "--check / --apply require --checkpoints-json")
    state, _ = live_preflight(client, manifest_path, m,
                              args.checkpoints_json.resolve(), allow_ready=True)
    if args.check:
        print(f"BATCH FINALIZE CHECK PASS: {m['batch_id']}; state={state}")
        print("READ-ONLY: no checkpoint, metadata, RAW, GCS, dbt or local files changed")
        return
    must(args.qa_confirmed, "--apply requires --qa-confirmed after 87/87 dbt QA PASS")
    if state == "COMMITTED":
        print("ALREADY_COMMITTED: no writes; use --export if needed")
        return
    print(f"BATCH FINALIZATION APPLY START: {m['batch_id']}")
    execute_apply(client, m)
    after, _ = live_preflight(client, manifest_path, m, None, allow_ready=False)
    must(after == "COMMITTED", "Post-commit verification failed; do NOT reapply blindly")
    print("BATCH FINALIZATION APPLY PASS: 4 committed checkpoints + 1 batch status")
    print("IMPORTANT: verified local checkpoint file is now STALE. Export and backup/swap it.")


def self_test() -> None:
    a = {t: "2026-11-01T10:00:00+00:00" for t in TABLES}
    b = {t: "2026-11-02T10:00:00+00:00" for t in TABLES}
    assert maps_match(a, a) and not maps_match(a, b)
    sql = transaction_sql()
    assert "BEGIN TRANSACTION" in sql and "COMMIT TRANSACTION" in sql
    assert "ROLLBACK TRANSACTION" in sql and "affected != 4" in sql
    for t in TABLES:
        assert f"@from_{t}" in sql and f"@to_{t}" in sql
    print("BATCH FINALIZER SELF-TEST PASS (no network, no writes)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    actions = p.add_mutually_exclusive_group(required=True)
    actions.add_argument("--self-test", action="store_true")
    actions.add_argument("--check", action="store_true")
    actions.add_argument("--apply", action="store_true")
    actions.add_argument("--export", action="store_true")
    p.add_argument("--manifest", type=Path)
    p.add_argument("--checkpoints-json", type=Path)
    p.add_argument("--output", type=Path)
    p.add_argument("--qa-confirmed", action="store_true")
    args = p.parse_args()
    if args.self_test:
        self_test()
        return
    if args.manifest is None:
        p.error("--check, --apply and --export require --manifest")
    if args.export and args.output is None:
        p.error("--export requires --output")
    if args.apply and not args.qa_confirmed:
        p.error("--apply requires --qa-confirmed (explicit human QA verification)")
    run(args)


if __name__ == "__main__":
    main()
