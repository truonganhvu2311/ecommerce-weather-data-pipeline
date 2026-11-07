NGAY 8 - BUOC 8.7.10.9: BATCH FINALIZATION + COMMITTED WATERMARKS

TRANG THAI: Chua duoc kiem chung tren live GCP. KHONG CHAY --apply truoc khi --check PASS.
Du an GCP: weather-data-project-512207; BigQuery location: asia-southeast1.
Batch: day8_customer_c990_20261104.

0) Dieu kien bat buoc:
   - Da xac nhan BigQuery RAW/registry/staging, audit PASS.
   - Da chay Airflow day8_dbt_build_manual SAU khi load C990, 87/87 PASS.
   - Khong co job loader/committer khac dang hoat dong.
   - Giu nguyen manifest.json status pending va 11 migration files cu.

1) Copy finalize_airflow_batch.py vao src/loading/ trong project.

2) Trong PowerShell tai project root:
   $batchId = 'day8_customer_c990_20261104'
   $manifest = ".\data\airflow_batches\$batchId\manifest.json"
   $checkpoint = '.\state\airflow\committed_watermarks_verified.json'
   $python = '.\.venv_gcp\Scripts\python.exe'
   & $python .\src\loading\finalize_airflow_batch.py --self-test
   & $python .\src\loading\finalize_airflow_batch.py --check --manifest $manifest --checkpoints-json $checkpoint
   Ky vong: BATCH FINALIZE CHECK PASS ... state=READY

3) Neu --check PASS va bang chung dbt 87/87 PASS la SAU lan load RAW:
   & $python .\src\loading\finalize_airflow_batch.py --apply --qa-confirmed --manifest $manifest --checkpoints-json $checkpoint
   Ky vong: BATCH FINALIZATION APPLY PASS
   Mot BigQuery multi-statement transaction update 4 committed watermarks
   va ghi 1 pipeline_batches.status='committed'.
   Neu loi hay khong chac chan: KHONG lap --apply vo dieu kien;
   kiem tra BigQuery state bang SQL.

4) Sau commit, checkpoint JSON cu la STALE. KHONG chay --prepare batch moi voi no.
   Xuat file moi, khong ghi de file cu:
   $next = '.\state\airflow\committed_watermarks_verified_next.json'
   & $python .\src\loading\finalize_airflow_batch.py --export --manifest $manifest --output $next
   Neu PASS:
   Copy-Item $checkpoint '.\state\airflow\committed_watermarks_before_c990.json' -ErrorAction Stop
   Move-Item $next $checkpoint -Force
   Luu y: file local checkpoint nay duoc ignore Git, khong push.

5) Chay day8_7_10_9_verify_commit.sql tren BigQuery region asia-southeast1.
   Ky vong: 4 rows watermark deu committed_batch_id=day8_customer_c990_20261104;
   1 pipeline_batches row status=committed;
   RAW C990 va registry van 1/1.

6) Khong chinh sua manifest local/GCS pending: no la ban ghi immutable cua batch.
   Trang thai chinh thuc duoc luu trong de_etl.pipeline_batches.

7) LUU Y: --qa-confirmed la xac nhan thu cong; script KHONG tu doc Airflow dbt logs.
   timestamp-based CDC voi cutoff-lag=0 khong bao dam tranh late commits trong san xuat.
