DAY 8.7.10.16 — RECOVERY SANDBOX & READ-ONLY CLOUD AUDIT
E-commerce Sales & Weather Analytics | Airflow 3.3.2

MUC TIEU
  - Test 8 diem gián đoạn/khôi phuc tren file + SQLite GIẢ LẬP.
  - Chup snapshot BigQuery THẬT truoc va sau bang SELECT, kiem tra RAW / registry / 4 watermarks khong thay doi.
  - KHONG ghi PostgreSQL/GCS/BigQuery/dbt/watermark that.
  - KHONG sua day8_auto_incremental_pipeline.py, KHONG bat schedule.

PHÂN BIET QUAN TRONG
  - BAI TEST NAY LA SIMULATION CUC BO, KHONG CHUNG MINH PHUC HOI KHI GCP THUC SU MAT KET NOI GIUA LENH GHI.
  - SQLite mo phong transaction RAW + registry va metadata/watermark; no KHONG phai BigQuery.
  - Fault injection dung exception de mo phong diem gian doan, khong kill container.

NOI DUNG ZIP
  src/testing/day8_partial_write_sandbox.py   : standard-library local test runner
  dags/day8_partial_failure_sandbox.py        : Airflow DAG 4 tasks, cloud READ ONLY
  README_day8_7_10_16.txt

BUOC 1 — COPY FILE VAO PROJECT (KHONG XOA FILE CU)
  C:\Users\ADMIN\OneDrive\Máy tính\data-engineering-project\src\testing\day8_partial_write_sandbox.py
  C:\Users\ADMIN\OneDrive\Máy tính\data-engineering-project\dags\day8_partial_failure_sandbox.py

BUOC 2 — TU KIEM TRA TAI WINDOWS (KHONG CO NETWORK/CLOUD)
  cd "C:\Users\ADMIN\OneDrive\Máy tính\data-engineering-project"
  .\.venv_gcp\Scripts\python.exe .\src\testing\day8_partial_write_sandbox.py --self-test

  KET QUA MONG MUON:
  DAY 8.7.10.16 LOCAL RECOVERY SANDBOX PASS: 8/8
  REAL POSTGRESQL/GCS/BIGQUERY/DBT/WATERMARKS: NO WRITES

BUOC 3 — COPY DAG
  Copy-Item .\dags\day8_partial_failure_sandbox.py "C:\Users\ADMIN\airflow-day8\dags\day8_partial_failure_sandbox.py" -Force

BUOC 4 — KIEM TRA WORKER THAY DUOC SCRIPT
  cd "C:\Users\ADMIN\airflow-day8"
  docker compose exec airflow-worker ls /opt/airflow/de_project/src/testing/day8_partial_write_sandbox.py
  docker compose exec airflow-dag-processor airflow dags list-import-errors

  Khong can Dockerfile, compose, volumes moi, neu src/ mount va .recovery_tests tu buoc truoc van hoat dong.

BUOC 5 — AIRFLOW UI
  http://localhost:8080/dags
  DAG: day8_partial_failure_sandbox
  Trigger MOT LAN (khong can JSON conf)
  Bat buoc: khong co DAG/loader khac dang GHI BigQuery trong luc test, de BEFORE/AFTER audit cong bang.

  4 TASK EXPECTED:
  - snapshot_real_cloud             Success: read-only latest committed auto batch
  - run_local_failure_scenarios     Success: 8/8 local simulation
  - verify_real_cloud_unchanged     Success: before == after
  - finish                          Success: summary

  FINISH LOG MONG MUON:
  DAY 8.7.10.16 SANDBOX RECOVERY PASS: 8/8 local fault-injection cases
  REAL BIGQUERY AUDIT: BEFORE == AFTER; NO CLOUD WRITES BY THIS DAG
  IMPORTANT: This simulates partial writes; real GCP mid-failure recovery is NOT proven

BAI TEST GOM 8 DIEU KIEN
  1) GCS partial upload, retry khong ghi de JSONL da ton tai, tao manifest 1 lan
  2) GCS existing checksum khac: fail closed, khong ghi de
  3) Transaction RAW ghi du lieu nhung chua registry thi rollback
  4) Transaction RAW+registry da commit nhung mat ACK, replay bo qua file da xac minh
  5) QA fail, cam watermark commit
  6) Commit 2/4 metadata rows roi loi thi rollback ca 4
  7) Metadata commit xong nhung mat ACK, retry no-op verify
  8) Export checkpoint khop 4 committed watermark mo phong

BAO CAO
  Bao cao JSON duoc luu trong thu muc runtime mount co the ghi:
  C:\Users\ADMIN\airflow-day8\runtime\airflow_batches_auto\.recovery_tests\day8_recovery_sandbox_<run-hash>.json
  Chi luu ket qua gia lap, khong co credentials.
  Sandbox data files + SQLite duoc tao trong TemporaryDirectory va xoa khi script ket thuc.

TEN ANH GOI Y
  docs/screenshots/day8_partial_failure_sandbox_4tasks.png
  docs/screenshots/day8_partial_failure_8_cases.png
  docs/screenshots/day8_recovery_cloud_unchanged.png

NEU CO LOI
  - Dung. Xem Logs task FAIL; KHONG trigger auto pipeline hoac sua watermark.
  - Neu snapshot_real_cloud FAIL: kiem tra latest batch status/4 watermarks/RAW/registry, khong can sua DAG tu dong.
  - Neu run_local_failure_scenarios FAIL: xem log script; KHONG sua du lieu GCP.
  - Neu verify_real_cloud_unchanged FAIL: co the co job GCP khac ghi trong luc test; kiem tra log truoc khi retry.

SAU KHI PASS
  - Co the tong ket Day 8 nhu portfolio demo co Airflow, test 87/87, empty/nonempty branch,
    cloud idempotency cho cac batch da test, Airflow retry va local fault-injection simulations.
  - KHONG duoc mo ta la san sang production; chua kiem chung full real-cloud crash recovery,
    late commits, hard DELETE CDC, concurrency, scheduled operation.
