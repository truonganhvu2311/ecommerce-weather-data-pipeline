"""Day 8.7.10.16: local-only fault injection for incremental batch safety.

Simulates GCS create-only objects and a BigQuery-like RAW/registry/watermark
transaction using local files + SQLite. Does NOT call GCP/PG or modify real
pipeline state. Faults are caught exceptions representing process interruption
at designated points; not a live-cloud crash/retry guarantee.

Python >=3.10, standard library only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

TABLES = ("customers", "products", "orders", "order_items")
BATCH = "sandbox_fault_batch"
PREVIOUS = "sandbox_previous_commit"
SOURCE_FILE = "airflow_batches/sandbox_fault_batch/raw/transactions/customers/customer_c990_v4.jsonl"
MANIFEST_FILE = "airflow_batches/sandbox_fault_batch/manifest.json"
BASE_WM = "2026-11-07T00:00:00+00:00"
NEXT_WM = "2026-11-08T00:00:00+00:00"


class SimulatedInterruption(RuntimeError):
    """Fault injection: pretend the process stopped at this boundary."""


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_bytes(doc: dict[str, Any]) -> bytes:
    return (json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def create_only_verified(root: Path, key: str, data: bytes) -> str:
    """Local create-if-absent analogue; fails closed on content mismatch."""
    if key.startswith("/") or "\\" in key or ".." in Path(key).parts:
        raise ValueError("unsafe object key")
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(data)
        state = "CREATED"
    except FileExistsError:
        state = "SKIP_VERIFIED"
    if sha256(path.read_bytes()) != sha256(data):
        raise ValueError("existing object checksum mismatch; do not overwrite")
    return state


def upload_mock(root: Path, payload: bytes, manifest: bytes, *, fault: bool = False) -> tuple[str, str]:
    raw_status = create_only_verified(root, SOURCE_FILE, payload)
    if fault:
        raise SimulatedInterruption("AFTER_GCS_JSONL_BEFORE_MANIFEST")
    manifest_status = create_only_verified(root, MANIFEST_FILE, manifest)
    return raw_status, manifest_status


def open_db(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(path))
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript("""
        CREATE TABLE raw_events (
          source_file TEXT NOT NULL,
          source_line INTEGER NOT NULL,
          batch_id TEXT NOT NULL,
          customer_id TEXT NOT NULL,
          customer_name TEXT NOT NULL,
          PRIMARY KEY(source_file, source_line)
        );
        CREATE TABLE loaded_files (
          source_file TEXT PRIMARY KEY,
          batch_id TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          record_count INTEGER NOT NULL
        );
        CREATE TABLE watermarks (
          source_table TEXT PRIMARY KEY,
          committed_watermark TEXT NOT NULL,
          committed_batch_id TEXT NOT NULL
        );
        CREATE TABLE pipeline_batches (
          batch_id TEXT PRIMARY KEY,
          status TEXT NOT NULL
        );
    """)
    db.executemany(
        "INSERT INTO watermarks VALUES(?,?,?)",
        [(t, BASE_WM, PREVIOUS) for t in TABLES],
    )
    db.execute("INSERT INTO pipeline_batches VALUES(?,?)", (BATCH, "pending"))
    db.commit()
    return db


def checkpoint_snapshot(db: sqlite3.Connection) -> list[tuple]:
    return db.execute(
        "SELECT source_table,committed_watermark,committed_batch_id "
        "FROM watermarks ORDER BY source_table"
    ).fetchall()


def counts(db: sqlite3.Connection) -> tuple[int, int]:
    return (
        db.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0],
        db.execute("SELECT COUNT(*) FROM loaded_files").fetchone()[0],
    )


def load_mock(db: sqlite3.Connection, *, checksum: str, fault: str | None = None) -> str:
    """One atomic RAW + registry transaction; replay verifies before skip."""
    try:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute(
            "SELECT sha256,record_count,batch_id FROM loaded_files WHERE source_file=?",
            (SOURCE_FILE,),
        ).fetchone()
        raw_count = db.execute(
            "SELECT COUNT(*) FROM raw_events WHERE source_file=?", (SOURCE_FILE,)
        ).fetchone()[0]
        if existing is not None:
            if existing != (checksum, 1, BATCH) or raw_count != 1:
                raise ValueError("RAW/registry mismatch; refuse unsafe replay")
            db.commit()
            return "SKIP_VERIFIED"
        if raw_count != 0:
            raise ValueError("orphan RAW event; refuse unsafe replay")
        db.execute(
            "INSERT INTO raw_events VALUES(?,?,?,?,?)",
            (SOURCE_FILE, 1, BATCH, "C990", "Airflow Test Customer V4"),
        )
        if fault == "before_registry":
            raise SimulatedInterruption("AFTER_RAW_INSERT_BEFORE_REGISTRY_COMMIT")
        db.execute("INSERT INTO loaded_files VALUES(?,?,?,?)", (SOURCE_FILE, BATCH, checksum, 1))
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    if fault == "after_commit_ack":
        raise SimulatedInterruption("AFTER_BQ_COMMIT_BEFORE_TASK_ACK")
    return "LOADED"


def batch_state(db: sqlite3.Connection) -> str:
    return db.execute("SELECT status FROM pipeline_batches WHERE batch_id=?", (BATCH,)).fetchone()[0]


def finalize_mock(db: sqlite3.Connection, *, qa_pass: bool, fault: str | None = None) -> str:
    if not qa_pass:
        raise RuntimeError("QA is not PASS: fail closed, do not commit watermark")
    try:
        db.execute("BEGIN IMMEDIATE")
        state = batch_state(db)
        snapshot = checkpoint_snapshot(db)
        if state == "committed":
            if any((w, b) != (NEXT_WM, BATCH) for _, w, b in snapshot):
                raise RuntimeError("committed batch watermark mismatch")
            db.commit()
            return "SKIP_VERIFIED"
        if state != "pending" or any((w, b) != (BASE_WM, PREVIOUS) for _, w, b in snapshot):
            raise RuntimeError("unexpected watermark/state: abort")
        if counts(db) != (1, 1):
            raise RuntimeError("RAW and registry not reconciled")
        for index, table in enumerate(TABLES):
            db.execute(
                "UPDATE watermarks SET committed_watermark=?,committed_batch_id=? WHERE source_table=?",
                (NEXT_WM, BATCH, table),
            )
            if fault == "during_watermark_tx" and index == 1:
                raise SimulatedInterruption("AFTER_TWO_WATERMARK_UPDATES_BEFORE_COMMIT")
        db.execute("UPDATE pipeline_batches SET status='committed' WHERE batch_id=?", (BATCH,))
        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise
    if fault == "after_commit_ack":
        raise SimulatedInterruption("AFTER_CHECKPOINT_COMMIT_BEFORE_ACK")
    return "COMMITTED"


def expect_interruption(fn, marker: str) -> None:
    try:
        fn()
    except SimulatedInterruption as exc:
        if str(exc) != marker:
            raise AssertionError(f"unexpected fault {exc}") from exc
    else:
        raise AssertionError(f"expected interruption {marker} did not happen")
    print(f"[INJECTED] {marker}")


def run_tests(sandbox: Path) -> dict[str, Any]:
    sandbox.mkdir(parents=True, exist_ok=True)
    root = sandbox / "mock_gcs"
    customer = {
        "customer_id": "C990", "customer_name": "Airflow Test Customer V4",
        "updated_at": NEXT_WM,
    }
    payload = json_bytes(customer)
    digest = sha256(payload)
    manifest = json_bytes({
        "batch_id": BATCH, "status": "pending",
        "files": [{"object_path": SOURCE_FILE, "sha256": digest, "record_count": 1}],
    })
    with open_db(sandbox / "mock_warehouse.sqlite") as db:
        before = checkpoint_snapshot(db)
        assert len(before) == 4 and counts(db) == (0, 0)
        checks: list[str] = []

        # 1) GCS object created, task ack lost before manifest: retry completes safely.
        expect_interruption(
            lambda: upload_mock(root, payload, manifest, fault=True),
            "AFTER_GCS_JSONL_BEFORE_MANIFEST",
        )
        assert (root / SOURCE_FILE).exists() and not (root / MANIFEST_FILE).exists()
        assert upload_mock(root, payload, manifest) == ("SKIP_VERIFIED", "CREATED")
        assert upload_mock(root, payload, manifest) == ("SKIP_VERIFIED", "SKIP_VERIFIED")
        checks.append("GCS_PARTIAL_UPLOAD_REPLAY_PASS")
        print("[PASS] GCS_PARTIAL_UPLOAD_REPLAY: verified existing RAW object; manifest created once")

        # 2) Already-present object with different content must fail closed.
        try:
            create_only_verified(root, SOURCE_FILE, payload + b"tampered")
        except ValueError as exc:
            assert "checksum mismatch" in str(exc)
        else:
            raise AssertionError("tampered GCS object should have been rejected")
        assert (root / SOURCE_FILE).read_bytes() == payload
        checks.append("GCS_MISMATCH_FAIL_CLOSED_PASS")
        print("[PASS] GCS_MISMATCH_FAIL_CLOSED: no overwrite")

        # 3) Partial RAW INSERT inside a transaction must roll back with registry.
        expect_interruption(
            lambda: load_mock(db, checksum=digest, fault="before_registry"),
            "AFTER_RAW_INSERT_BEFORE_REGISTRY_COMMIT",
        )
        assert counts(db) == (0, 0) and checkpoint_snapshot(db) == before
        checks.append("RAW_REGISTRY_ROLLBACK_PASS")
        print("[PASS] RAW_REGISTRY_ROLLBACK: zero RAW, zero registry after injected failure")

        # 4) DB transaction committed but caller lost ACK: repeat sees same row once.
        expect_interruption(
            lambda: load_mock(db, checksum=digest, fault="after_commit_ack"),
            "AFTER_BQ_COMMIT_BEFORE_TASK_ACK",
        )
        assert counts(db) == (1, 1) and checkpoint_snapshot(db) == before
        assert load_mock(db, checksum=digest) == "SKIP_VERIFIED"
        assert counts(db) == (1, 1)
        checks.append("BQ_POST_COMMIT_ACK_REPLAY_PASS")
        print("[PASS] BQ_POST_COMMIT_ACK_REPLAY: one RAW row + one registry entry, no duplicates")

        # 5) Downstream QA failure must keep watermark unchanged.
        try:
            finalize_mock(db, qa_pass=False)
        except RuntimeError as exc:
            assert "QA is not PASS" in str(exc)
        else:
            raise AssertionError("QA failure must block commit")
        assert checkpoint_snapshot(db) == before and batch_state(db) == "pending"
        checks.append("QA_FAILURE_BLOCKS_COMMIT_PASS")
        print("[PASS] QA_FAILURE_BLOCKS_COMMIT: all four watermarks unchanged")

        # 6) Failure after two metadata updates but before commit must roll back all.
        expect_interruption(
            lambda: finalize_mock(db, qa_pass=True, fault="during_watermark_tx"),
            "AFTER_TWO_WATERMARK_UPDATES_BEFORE_COMMIT",
        )
        assert checkpoint_snapshot(db) == before and batch_state(db) == "pending"
        checks.append("WATERMARK_TRANSACTION_ROLLBACK_PASS")
        print("[PASS] WATERMARK_TRANSACTION_ROLLBACK: four checkpoints unchanged")

        # 7) Checkpoint tx committed; task ACK lost; replay is a verified no-op.
        expect_interruption(
            lambda: finalize_mock(db, qa_pass=True, fault="after_commit_ack"),
            "AFTER_CHECKPOINT_COMMIT_BEFORE_ACK",
        )
        assert batch_state(db) == "committed"
        assert finalize_mock(db, qa_pass=True) == "SKIP_VERIFIED"
        after = checkpoint_snapshot(db)
        assert len(after) == 4 and all((w, b) == (NEXT_WM, BATCH) for _, w, b in after)
        assert counts(db) == (1, 1)
        checks.append("CHECKPOINT_POST_COMMIT_ACK_REPLAY_PASS")
        print("[PASS] CHECKPOINT_POST_COMMIT_ACK_REPLAY: four consistent committed checkpoints")

        # 8) Simulated checkpoint export reconciled with committed state.
        export = {
            "source": "mock_sqlite_watermarks", "verified": True,
            "committed_batch_id": BATCH,
            "committed_watermarks": {name: stamp for name, stamp, _ in after},
        }
        (sandbox / "checkpoint_after.json").write_bytes(json_bytes(export))
        readback = json.loads((sandbox / "checkpoint_after.json").read_text(encoding="utf-8"))
        assert readback == export
        checks.append("CHECKPOINT_EXPORT_PASS")
        print("[PASS] CHECKPOINT_EXPORT: matched all four committed values")

        report = {
            "kind": "LOCAL_SIMULATION_ONLY",
            "scope": "local temporary files and SQLite, no real cloud writes",
            "batch_id": BATCH,
            "scenarios_passed": len(checks),
            "checks": checks,
            "simulated_raw_rows": counts(db)[0],
            "simulated_registry_entries": counts(db)[1],
            "simulated_watermarks": len(after),
            "batch_state": batch_state(db),
        }
        print("DAY 8.7.10.16 LOCAL RECOVERY SANDBOX PASS: 8/8")
        print("REAL POSTGRESQL/GCS/BIGQUERY/DBT/WATERMARKS: NO WRITES")
        return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--run", action="store_true")
    parser.add_argument("--output", type=Path, help="optional JSON audit report path")
    args = parser.parse_args()
    if args.self_test and args.output:
        parser.error("--self-test cannot take --output")
    with tempfile.TemporaryDirectory(prefix="day8_local_fault_sandbox_") as tmp:
        report = run_tests(Path(tmp))
    if args.run and args.output:
        path = args.output
        path.parent.mkdir(parents=True, exist_ok=True)
        # Create-only report; never silently replace previously saved evidence.
        with path.open("x", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        print(f"SANDBOX_REPORT_WRITTEN: {path}")


if __name__ == "__main__":
    main()
