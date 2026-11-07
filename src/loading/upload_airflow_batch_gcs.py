"""GCS uploader for immutable PostgreSQL incremental batches (Day 8, step 8.7.10.4).

Modes:
    --self-test  Local path/prefix checks, no network or writes.
    --check      Validate local batch + committed checkpoint; inspect GCS only.
    --upload     Explicit, create-only upload for NONEMPTY prepared batch.

Never touches old migration objects, BigQuery, the source database or watermarks.
Never use this as a substitute for the BigQuery batch loader / end-to-end QA.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

PROJECT_ID = "weather-data-project-512207"
BUCKET_NAME = "weather-data-project-512207-raw"

# Existing validator was built and tested in the previous Airflow preflight.
INGESTION_DIR = Path(__file__).resolve().parents[1] / "ingestion"
sys.path.insert(0, str(INGESTION_DIR))
from validate_batch_manifest import validate  # noqa: E402


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def gcs_object_name(batch_id: str, relative: str) -> str:
    if relative == "manifest.json":
        return f"airflow_batches/{batch_id}/manifest.json"
    # The validator has already approved this exact directory structure.
    if not relative.startswith("raw/transactions/") or ".." in Path(relative).parts:
        raise ValueError(f"Unsafe object path: {relative}")
    return f"airflow_batches/{batch_id}/{relative}"


def verify_remote(blob, expected: bytes, label: str) -> None:
    """Verify actual GCS bytes, not just object size or user-set metadata."""
    remote = blob.download_as_bytes()
    if len(remote) != len(expected) or sha256(remote) != sha256(expected):
        raise RuntimeError(f"GCS OBJECT CONFLICT (no overwrite): {label}")


def inspect_bucket(bucket, manifest: dict, batch_dir: Path) -> tuple[dict, dict]:
    batch_id = manifest["batch_id"]
    prefix = f"airflow_batches/{batch_id}/"
    desired = {}
    for item in manifest["files"]:
        path = item["local_path"]
        desired[gcs_object_name(batch_id, item["object_path"])] = (
            (batch_dir / path).read_bytes(), "application/x-ndjson"
        )
    desired[gcs_object_name(batch_id, "manifest.json")] = (
        (batch_dir / "manifest.json").read_bytes(), "application/json"
    )

    existing = {blob.name: blob for blob in bucket.list_blobs(prefix=prefix)}
    unexpected = sorted(set(existing) - set(desired))
    if unexpected:
        raise RuntimeError(f"Unexpected objects in this isolated batch prefix: {unexpected}")
    for name, blob in existing.items():
        verify_remote(blob, desired[name][0], name)

    if not manifest["files"]:
        # Empty batches intentionally have NO GCS artifacts. There is no data to upload.
        if existing:
            raise RuntimeError("EMPTY_BATCH unexpectedly has objects on GCS")
        return {}, {}

    # A manifest is uploaded LAST. If already present, all transaction files must exist.
    manifest_name = gcs_object_name(batch_id, "manifest.json")
    if manifest_name in existing:
        missing_data = sorted(set(desired) - set(existing))
        if missing_data:
            raise RuntimeError("Manifest exists but transaction objects are missing: " + str(missing_data))
    return desired, existing


def upload_if_missing(bucket, name: str, data: bytes, content_type: str) -> str:
    """Safe replay guard: never overwrite an existing GCS generation."""
    from google.api_core.exceptions import PreconditionFailed

    blob = bucket.blob(name)
    try:
        blob.upload_from_string(
            data,
            content_type=content_type,
            if_generation_match=0,
        )
        operation = "UPLOADED"
    except PreconditionFailed:
        # A previous attempt may have uploaded it before a crash / network timeout.
        operation = "ALREADY_EXISTS"
    verify_remote(bucket.blob(name), data, name)
    return operation


def execute(manifest_path: Path, checkpoint_path: Path, upload: bool) -> None:
    # Important: fail before any cloud mutation if local batch is not valid.
    result = validate(manifest_path, checkpoint_path)
    manifest_path = manifest_path.resolve()
    batch_dir = manifest_path.parent
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert result["batch_id"] == manifest["batch_id"]

    # All operations hereafter are confined to the isolated GCS prefix.
    from google.cloud import storage

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    desired, existing = inspect_bucket(bucket, manifest, batch_dir)

    if not manifest["files"]:
        print(f"EMPTY_BATCH: {manifest['batch_id']}; 0 files/0 rows; GCS upload skipped")
        print("NO GCS WRITES; no BigQuery load or watermark commit")
        return

    missing = sorted(set(desired) - set(existing))
    if not upload:
        print(f"GCS CHECK PASSED: batch={manifest['batch_id']}")
        print(f"Existing verified objects={len(existing)}, missing objects={len(missing)}")
        for name in missing:
            print("[NOT_YET_UPLOADED]", name)
        print("READ-ONLY: No GCS objects created or modified")
        return

    # Upload data before manifest. A created manifest marks only storage completeness,
    # NOT downstream load success, committed checkpoint, or a production-ready batch.
    manifest_name = gcs_object_name(manifest["batch_id"], "manifest.json")
    for name in sorted(desired):
        if name == manifest_name:
            continue
        if name in existing:
            print("[SKIP_VERIFIED]", name)
            continue
        data, kind = desired[name]
        print(f"[{upload_if_missing(bucket, name, data, kind)}] {name}")

    # Re-verify all data before publishing the manifest.
    for name, (data, _) in desired.items():
        if name != manifest_name:
            verify_remote(bucket.blob(name), data, name)
    if manifest_name not in existing:
        data, kind = desired[manifest_name]
        print(f"[{upload_if_missing(bucket, manifest_name, data, kind)}] {manifest_name}")
    else:
        print("[SKIP_VERIFIED]", manifest_name)
    print("GCS UPLOAD VERIFIED; BigQuery loading/QA/watermark NOT performed")


def self_test() -> None:
    assert gcs_object_name("demo_01", "raw/transactions/orders/dt=2026-11-04/orders_demo_01.jsonl") == (
        "airflow_batches/demo_01/raw/transactions/orders/dt=2026-11-04/orders_demo_01.jsonl"
    )
    assert gcs_object_name("demo_01", "manifest.json") == "airflow_batches/demo_01/manifest.json"
    assert sha256(b"test") == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    try:
        gcs_object_name("demo_01", "../secrets.txt")
    except ValueError:
        pass
    else:
        raise AssertionError("Unsafe object key was accepted")
    print("GCS UPLOADER SELF-TEST PASSED (no network, no writes)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--self-test", action="store_true")
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--upload", action="store_true", help="Explicit GCS create-only writes (nonempty batch only)")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--checkpoints-json", type=Path)
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.manifest or not args.checkpoints_json:
        parser.error("--check and --upload require --manifest and --checkpoints-json")
    execute(args.manifest, args.checkpoints_json, upload=args.upload)


if __name__ == "__main__":
    main()
