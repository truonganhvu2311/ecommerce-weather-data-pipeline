# Ngày 5 (bổ sung GCP) — Nạp RAW từ GCS vào BigQuery

Bộ tài liệu hỗ trợ dự án E-commerce Sales & Weather Analytics, Project ID `weather-data-project-512207`.

## Điều kiện trước khi chạy

- Đã tạo 5 RAW tables dưới `de_raw` và bảng `de_etl.loaded_files` theo file SQL ngày 5.
- Đã upload 8 JSONL + 3 JSON vào `gs://weather-data-project-512207-raw/raw/`.
- Đã upload `manifests/gcs_migration_manifest.csv` chứa 11 dòng, các cột: `object_path,size_bytes,sha256`.
- Cả GCS và BigQuery datasets nằm tại Singapore (`asia-southeast1`).
- Đây là đợt **migration snapshot ban đầu**, không phải pipeline CDC chạy theo lịch.

## Chuẩn bị trên Windows PowerShell tại thư mục gốc repository

```powershell
py -3 -m venv .venv_gcp
.\.venv_gcp\Scripts\python.exe -m pip install --upgrade pip
.\.venv_gcp\Scripts\python.exe -m pip install -r .\requirements-gcp.txt
```

Nếu chưa cài Google Cloud CLI, cài từ https://cloud.google.com/sdk/docs/install và mở PowerShell mới.

```powershell
gcloud --version
gcloud auth login
gcloud config set project weather-data-project-512207
gcloud auth application-default login
gcloud auth application-default set-quota-project weather-data-project-512207
```

**Không chia sẻ access token, authorization code, service-account keys hoặc file ADC với người khác; không commit chúng.**

## Chạy kiểm tra không ghi dữ liệu

```powershell
.\.venv_gcp\Scripts\python.exe .\src\loading\load_raw_to_bigquery.py --check
```

Kỳ vọng `CHECK PASSED: 11/11 files + RAW schemas`. Nếu có `MISSING`, `SHA256 MISMATCH`, `Wrong schema` hoặc `403`, xử lý lỗi trước khi apply.

## Nạp dữ liệu (chỉ sau khi check thành công)

```powershell
.\.venv_gcp\Scripts\python.exe .\src\loading\load_raw_to_bigquery.py --apply
```

Chạy lại cùng lệnh để test idempotency; lần thứ hai lý tưởng `loaded_files=0, skipped_files=11, new_rows=0`.

## Đối soát

Mở BigQuery Studio tại `https://console.cloud.google.com/bigquery?project=weather-data-project-512207` và chạy từng query trong `sql/day5_bigquery_reconciliation.sql`.

Snapshot dự kiến (chỉ nếu giữ đúng 11 files của PostgreSQL cũ): customers=5, products=6, orders=11, order_items=16, weather=3; tổng RAW = 41.

Nếu số liệu khác, dừng tại bước đối soát và kiểm tra; **không xóa hoặc truncate bảng**. Không chỉnh `state/watermark.json` hay các bảng checkpoint.

## Thiết kế và giới hạn

- 11 files được xác minh trước khi có ghi dữ liệu.
- Mỗi file qua `de_etl._stage_*`; load job ghi stage, sau đó SQL transaction đưa dữ liệu RAW và registry vào cùng một commit.
- Khi rerun, loader so SHA-256, số bản ghi và registry để quyết định skip.
- Không dùng song song hai phiên loader trên cùng RAW/registry. Chúng ta sẽ giải quyết race conditions và state của các batch mới khi xây Airflow.
- Nếu một file thất bại, các file trước đó có thể đã commit; rerun sẽ tiếp tục mà không nạp lại những file đã được registry ghi nhận.
- `de_etl.pipeline_batches` và `de_etl.watermarks` chưa bị thay đổi.
- Nếu drop bảng stage lỗi sau khi commit, loader báo warning; bảng stage có thể được dọn sau khi xác minh không còn job sử dụng.
