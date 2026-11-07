# Ngày 8 — Apache Airflow Orchestration: Tổng kết & bàn giao

**Dự án:** E-commerce Sales & Weather Analytics Pipeline  
**Mốc tổng kết:** 08/11/2026  
**Trạng thái:** Đã hoàn tất các bài thực hành Ngày 8 cho mục tiêu demo/portfolio, theo kết quả người thực hiện đã xác nhận; **chưa coi là vận hành production**.

> Phạm vi xác minh: bản này dựa vào các lệnh, ảnh và kết quả PASS người thực hiện đã trao đổi. Chưa kiểm kê độc lập toàn bộ mã nguồn và trạng thái mới nhất của Git/GCP. Cần kiểm tra `git status`, tên screenshot và các nguồn dữ liệu thực tế trước khi công bố.

## 1. Kiến trúc đã thực hành

```text
PostgreSQL (ecommerce_source: customers/products/orders/order_items)
      |
      v
Airflow: prepare_batch (incremental timestamp + committed watermarks)
      |
      +--- empty batch ---> finish_empty_batch (no cloud writes)
      |
      +--- non-empty batch
             |
             v
       JSONL + immutable manifest + SHA-256
             |
             v
       GCS bucket: weather-data-project-512207-raw
             |
             v
       BigQuery de_raw + de_etl.loaded_files (file-level replay)
             |
             v
       dbt BigQuery: 11 models + 76 tests
             |
             v
       QA + de_etl.pipeline_batches / de_etl.watermarks
             |
             v
       Per-run checkpoint export
```

- GCP project: `weather-data-project-512207`; BigQuery region: `asia-southeast1`.
- Airflow: Apache Airflow `3.3.2`, sử dụng Docker Compose, WSL2 và Google Cloud Provider.
- dbt Core `1.12.6`, dbt BigQuery adapter `1.12.1`; môi trường tách khỏi Python chính của Airflow.
- Layers: `de_raw`, `de_staging`, `de_intermediate`, `de_marts`; metadata: `de_etl`.
- DAG end-to-end: `day8_auto_incremental_pipeline`, **manual trigger**, `schedule=None`.

## 2. Các mốc đã hoàn thành

| Giai đoạn | Kết quả người thực hiện xác nhận |
|---|---|
| Docker/WSL2/Airflow | Khởi tạo thành công, UI và các dịch vụ hoạt động |
| Smoke test, integration preflight | DAG chạy PASS |
| PostgreSQL source | Kết nối read-only PASS |
| Google Cloud GCS/BigQuery | ADC, provider và preflight PASS |
| dbt integration | `dbt debug` PASS; `dbt ls`: 11 models, 76 tests |
| Airflow điều phối dbt | `dbt build`: 87/87 PASS |
| Batch rỗng | Validator và GCS uploader check PASS; không upload dữ liệu rỗng |
| Batch INSERT C990 | PostgreSQL → JSONL → GCS → BigQuery RAW + registry; đã finalize/commit checkpoint |
| Batch UPDATE C990 V2 | DAG prepared-batch 6/6 PASS; staging latest-state đúng; finalize/commit checkpoint |
| Auto pipeline nhánh rỗng | `prepare_batch`, `route_batch`, `finish_empty_batch` Success; các tác vụ ghi Skipped đúng kỳ vọng |
| Auto pipeline nhánh có dữ liệu (C990 V3) | Extraction → GCS → RAW → dbt 87/87 → checkpoint PASS; đã lưu bằng chứng |
| Retry drill | 4/4 tasks PASS, mô phỏng một lỗi tạm thời rồi retry thành công |
| Partial-write recovery sandbox | 8/8 tình huống PASS, 4/4 task PASS; BigQuery thật không đổi theo bài kiểm tra |

### Bằng chứng thực hành

Các tên dưới đây là **tên ảnh được đề xuất trong quá trình học**, không khẳng định tất cả là tên thực tế hiện lưu. Đối chiếu với `docs/screenshots/` trước khi viết README.

- `day8_dbt_build_airflow.png`, `day8_dbt_build_logs.png`
- `day8_gcs_batch_uploaded.png`, `day8_gcs_batch_check_passed.png`, `day8_gcs_batch_idempotency_passed.png`
- `day8_bigquery_downstream_qa_passed.png`, `day8_dbt_build_after_incremental.png`, `day8_dbt_qa_after_incremental.png`
- `day8_c990_update_finalization_passed.png`, `day8_c990_update_checkpoint_verified.png`, `day8_c990_update_watermarks_committed.png`
- `day8_incremental_pipeline_airflow.png`, `day8_incremental_pipeline_qa.png`
- `day8_auto_pipeline_empty_batch_success.png`
- `day8_auto_pipeline_nonempty_success.png`, `day8_auto_pipeline_dbt_87_pass.png`, `day8_auto_pipeline_watermark_commit.png`, `day8_auto_pipeline_staging_c990_v3.png`
- `day8_recovery_drill_success.png`, `day8_recovery_retry_log.png`, `day8_recovery_metadata_unchanged.png`
- `day8_partial_failure_sandbox_4tasks.png`, `day8_partial_failure_8_cases.png`, `day8_recovery_cloud_unchanged.png`

## 3. Scripts/DAGs quan trọng trong repo

**DAGs (thư mục `dags/`):**

- `day8_smoke_test.py`
- `day8_integration_preflight.py`
- `day8_postgres_readonly.py`
- `day8_gcp_readonly.py`
- `day8_dbt_orchestration_preflight.py`
- `day8_dbt_build_manual.py`
- `day8_batch_manifest_preflight.py`
- `day8_gcs_batch_check.py`
- `day8_batch_pipeline_manual.py`
- `day8_auto_runtime_preflight.py`
- `day8_auto_incremental_pipeline.py`
- `day8_recovery_retry_drill.py`
- `day8_partial_failure_sandbox.py`

**Python utilities:**

- `src/ingestion/extract_incremental_airflow.py` — chuẩn bị batch bất biến; không ghi watermark cũ khi chỉ extract.
- `src/ingestion/validate_batch_manifest.py` — manifest/SHA-256/checkpoints.
- `src/ingestion/export_committed_watermarks.py` — xuất snapshot checkpoint đã xác minh.
- `src/loading/upload_airflow_batch_gcs.py` — GCS uploader theo batch với kiểm tra object đã tồn tại.
- `src/loading/load_airflow_batch_bigquery.py` — loader RAW + registry theo batch.
- `src/loading/finalize_airflow_batch.py` — finalize metadata, commit watermarks sau QA, export checkpoint.
- `src/testing/day8_partial_write_sandbox.py` — recovery simulation SQLite/local.

Lưu ý: file `src/loading/load_raw_to_bigquery.py` cũ dành cho **11 file migration ban đầu**; giữ lại, không dùng làm batch loader mới. `src/ingestion/extract_incremental.py` cũ lưu watermark quá sớm, không dùng cho orchestration tự động.

## 4. Đặc điểm kỹ thuật cần giải thích được khi phỏng vấn

1. **Incremental extraction:** lọc theo `updated_at`, committed watermark và cutoff. Đây là timestamp-based CDC simulation, **không phải** WAL CDC.
2. **Immutable batch:** manifest, JSONL và SHA-256 giúp replay/đối soát theo từng batch.
3. **Idempotency cấp file:** uploader kiểm tra object và loader đối chiếu registry/hash trước khi ghi lại. Kết quả kiểm thử không thay cho bảo đảm tuyệt đối trước mọi concurrency/crash.
4. **Metadata commit cuối:** committed watermark chỉ cập nhật sau dbt QA đạt yêu cầu; BigQuery là nguồn checkpoint chuẩn cho DAG auto.
5. **Branching:** khi không có dữ liệu mới, pipeline bỏ qua các bước cloud/write; `Skipped` là kết quả mong đợi.
6. **Data quality:** `dbt build` kiểm tra 11 models và 76 tests (87/87) tại thời điểm các lần chạy được xác nhận.
7. **Recovery:** kiểm tra task retry thực tế trong Airflow; fault injection ghi dở chủ yếu **giả lập local/SQLite**, không phải kiểm thử gián đoạn cloud thật.

## 5. Giới hạn cần công bố minh bạch

- DAG end-to-end vẫn `schedule=None`, chạy thủ công; **chưa chứng minh vận hành định kỳ ổn định**.
- Timestamp-based CDC chưa giải quyết triệt để late commits, hard DELETE và mọi race condition quanh watermark.
- Chưa kiểm thử đầy đủ trường hợp crash giữa các thao tác thật trên GCS/BigQuery dưới tải/concurrency; recovery sandbox không tương đương production chaos test.
- dbt models là BigQuery views; kết quả có thể phản ánh RAW mới khi query lại mà không cần `dbt build`, trong khi build giúp chạy lại QA.
- Không tuyên bố exactly-once end-to-end hoặc production-ready khi chưa có kiểm thử/kiểm soát bổ sung.

## 6. Checklist đóng Ngày 8 và GitHub

- [ ] `git status --short` và `git diff --stat`: xem toàn bộ thay đổi trước khi stage.
- [ ] Kiểm tra `.gitignore` và `git status --short --untracked-files=all`; không stage `.env`, ADC JSON, `profiles.yml`, passwords, `.venv*`, Airflow logs, `state/watermark.json`, `state/airflow/*.json`, `data/airflow_batches/*`, `runtime/`.
- [ ] Đọc lại mọi file YAML/SQL/Python mới và kiểm tra không hardcode secrets.
- [ ] Chỉ stage có chọn lọc các DAG, scripts, Dockerfiles/Compose **mẫu đã được làm sạch**, documentation và screenshots phù hợp.
- [ ] Kiểm tra `git diff --cached --check` và `git diff --cached --name-only` trước khi commit.
- [ ] Commit với thông điệp rõ ràng, push nhánh hiện tại và xác nhận ảnh trên GitHub hiển thị đúng.
- [ ] Giữ local runtime, credentials và checkpoint nằm ngoài Git.

> **Đặc biệt:** Không đưa `docker-compose.override.yaml` lên GitHub nguyên trạng nếu chứa Windows absolute paths, cấu hình cá nhân hoặc secrets. Có thể cung cấp file `.example` với biến môi trường/mount được mô tả trong README.

## 7. Hướng sang Ngày 9 — Looker Studio

**Mục tiêu:** xây dashboard phân tích doanh thu và tác động/tham chiếu thời tiết trên các bảng marts của BigQuery.

1. Khảo sát schema, dữ liệu mẫu và grain của `de_marts.fct_orders`, `de_marts.fct_order_items`, `de_marts.mart_city_day`.
2. Kiểm tra các chỉ số: doanh thu đơn eligible, số đơn, AOV, top city/product, xu hướng theo ngày, rainfall/temperature (theo dữ liệu có thật).
3. Kết nối Looker Studio đến BigQuery, chọn mart/views phù hợp; kiểm soát field type, metric aggregation và filters.
4. Tạo dashboard dễ đọc: overview KPI, trend, city/product breakdown, weather comparison, bộ lọc ngày/thành phố.
5. Kiểm tra số liệu dashboard so với BigQuery SQL; chụp ảnh, ghi assumptions và limitations vào README.

**Chú ý:** Không khẳng định thời tiết *gây ra* thay đổi doanh thu; với dữ liệu demo ít dòng, chỉ trình bày khám phá và mối liên hệ mô tả, không suy luận nhân quả.

## 8. Cách diễn đạt phù hợp cho portfolio/CV

> Built an Airflow-orchestrated incremental ELT pipeline integrating PostgreSQL, Google Cloud Storage, BigQuery, and dbt. Implemented immutable batch manifests, SHA-256 verification, file-level replay safeguards, and downstream-gated watermark commits; validated 11 dbt models and 76 tests. Demonstrated empty/non-empty batch handling and retry/recovery behaviors in controlled tests.

Không gắn nhãn "production-grade", "fully scheduled" hoặc "exactly-once" nếu không có bằng chứng triển khai tương ứng.
