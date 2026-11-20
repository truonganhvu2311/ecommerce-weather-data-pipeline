# BÀN GIAO CUỐI CÙNG — DATA ENGINEERING PROJECT (NGÀY 1–10)

**Project:** E-commerce Sales & Weather Analytics Pipeline

**Ngày chốt:** 20/11/2026 (giờ Việt Nam)

**Trạng thái:** **HOÀN THÀNH ở mức học tập / demo / portfolio Data Engineer** theo các bằng chứng, ảnh và lệnh người thực hiện cung cấp trong quá trình làm dự án. **Không phải hệ thống production-ready.**

**Repository:** https://github.com/truonganhvu2311/ecommerce-weather-data-pipeline

**Phạm vi kiểm chứng của báo cáo:** Kết hợp bản bàn giao Ngày 1–9, kết quả SQL/Looker Studio, ảnh kiểm thử Airflow/dbt/Docker và Git do người thực hiện cung cấp đến hết Ngày 10. Một số phép kiểm tra đã được quan sát qua ảnh chụp/đầu ra Terminal, không phải kiểm toán độc lập toàn bộ tài nguyên cloud tại thời điểm viết. Số liệu thay đổi theo dữ liệu được đánh dấu **[SNAPSHOT]**.

---

## 1. Executive summary

Dự án xây dựng pipeline ELT để liên kết giao dịch thương mại điện tử từ PostgreSQL với dữ liệu thời tiết lịch sử từ Open-Meteo, lưu raw trên Google Cloud Storage, chuyển đổi và kiểm thử trong BigQuery/dbt, điều phối bằng Apache Airflow/Docker và trực quan hóa trên Looker Studio.

**Những năng lực đã được thể hiện:**

- Thiết kế nguồn OLTP bốn bảng và mô phỏng INSERT/UPDATE/soft-delete.
- Python timestamp-based incremental extraction, batch manifests, checksum SHA-256 và watermark.
- GCS RAW → BigQuery (`de_raw`, `de_staging`, `de_intermediate`, `de_marts`) với metadata trong `de_etl`.
- 11 mô hình dbt BigQuery và 76 bài kiểm thử; **87/87 successful executions** trong lần chạy được ghi nhận.
- Airflow 3.3.2 trên Docker, pipeline giao dịch **manual-trigger**, nhánh batch rỗng/có dữ liệu, QA-gated checkpoint và recovery drills.
- 3 trang Looker Studio, 5 business insights, đối soát metrics giữa fact orders và city-day mart.
- GitHub portfolio: README tiếng Anh, kiến trúc, từ điển dữ liệu, KPI definitions, mẫu Docker/Compose không chứa credentials, ảnh chứng minh và kịch bản demo/Q&A.

**Kết luận:** Đã đủ để giới thiệu một project cá nhân theo hướng **Data Engineer Intern / Fresher**, miễn là trình bày đúng giới hạn của demo và không tự nhận là production CDC hoặc fully automated.

## 2. Kiến trúc đã triển khai

### 2.1. Nguồn và các thành phần chính

| Thành phần | Thiết kế / công cụ | Trạng thái |
|---|---|---|
| Transaction source | PostgreSQL `ecommerce_source`: `customers`, `products`, `orders`, `order_items` | Đã thực hiện |
| External data | Open-Meteo Historical Weather API | Đã ingest lịch sử, tách với DAG giao dịch |
| Extract | Python, `updated_at`, soft-delete, JSONL, manifest/checksum | Đã thực hiện |
| Storage | GCS raw bucket `weather-data-project-512207-raw` | Đã kiểm tra upload/checksum/replay |
| Warehouse | Google BigQuery, project `weather-data-project-512207`, region `asia-southeast1` | Đã tạo raw/metadata và marts |
| Transform & QA | dbt Core `1.12.6`, dbt BigQuery `1.12.1` | 11 models + 76 tests ghi nhận PASS |
| Orchestration | Airflow `3.3.2`, Docker Desktop/WSL2 | DAG manual-trigger, QA gate |
| Reporting | Looker Studio | 3 trang dashboard |
| Version control | GitHub `master` | Đồng bộ tại lần kiểm tra cuối |

### 2.2. Data flow

1. PostgreSQL nhận các thay đổi giao dịch; extractor đọc các bản ghi theo thời gian cập nhật và checkpoint.
2. Extractor tạo batch JSONL cùng metadata, manifest và thông tin kiểm tra checksum.
3. Các file được lưu trong GCS RAW; BigQuery lưu cả raw entities và metadata nạp file/batch.
4. dbt xử lý latest state, staging, intermediate và analytical marts; chạy test để kiểm tra chất lượng.
5. Nhánh Airflow giao dịch chỉ commit watermark chính thức sau khi các bước nạp và QA đạt điều kiện.
6. Looker Studio đọc dữ liệu từ ba mart để dựng biểu đồ và báo cáo.
7. Open-Meteo historical ingestion được thực hiện **riêng**; DAG giao dịch hiện không tự lấy thời tiết mới ở mỗi lần chạy.

**Metadata quan trọng:** `de_etl.watermarks` là **nguồn checkpoint chính thức**; không dùng file JSON checkpoint cũ để ghi đè hoặc tự ý reset trạng thái đã commit.

### 2.3. BigQuery datasets và analytical grain

| Dataset | Vai trò |
|---|---|
| `de_raw` | Dữ liệu raw từ giao dịch và API thời tiết |
| `de_etl` | `loaded_files`, `pipeline_batches`, `watermarks` |
| `de_staging` | Làm sạch, chuẩn hóa, latest-state nguồn |
| `de_intermediate` | Kết hợp và lọc đơn đủ điều kiện, chuẩn hóa dữ liệu trung gian |
| `de_marts` | `fct_orders`, `fct_order_items`, `mart_city_day` |

- `fct_orders`: 1 dòng cho một **eligible order**.
- `fct_order_items`: 1 dòng cho một **eligible order item**.
- `mart_city_day`: 1 dòng cho mỗi **city/date**.
- Order amount/AOV, item line amount và city-day rollup là các loại metric có grain khác nhau; không join order–item 1:N rồi SUM order total.
- Business date của đơn sử dụng múi giờ `Asia/Ho_Chi_Minh`.
- Weather `NULL/unknown` khác với ngày khô (0 mm).

## 3. Tiến độ theo Ngày 1–10

**Lưu ý:** Đây là các mốc theo handbook, không khẳng định mọi công việc đều xảy ra trong đúng một ngày lịch.

| Mốc | Thành quả chính | Kết quả |
|---|---|---|
| **Ngày 1** | Chọn bài toán, business questions, kiến trúc sơ bộ, thiết kế transactional schema | Hoàn thành |
| **Ngày 2** | PostgreSQL source bốn bảng và dữ liệu mẫu | Hoàn thành |
| **Ngày 3** | Mô phỏng INSERT/UPDATE/soft-delete; Python timestamp-based incremental và JSONL | Hoàn thành |
| **Ngày 4** | Open-Meteo historical API, dữ liệu thời tiết city-day | Hoàn thành |
| **Ngày 5** | RAW/warehouse DEV qua Hugging Face Private và PostgreSQL khi GCS chưa sẵn sàng; metadata nạp file | Hoàn thành mốc DEV, giữ legacy |
| **Ngày 6** | dbt-postgres staging/tests; khóa phiên bản dependency và Git local | Hoàn thành mốc DEV |
| **Ngày 7** | Intermediate/facts/marts, tests/docs/lineage, GitHub; đồng thời chuyển dần sang GCS/BigQuery | Hoàn thành |
| **Cloud migration** | GCS RAW, BigQuery datasets/metadata, dbt BigQuery và quality checks | Hoàn thành |
| **Ngày 8** | Airflow/Docker, empty/non-empty branch, watermark commit sau QA, retry/recovery drills | Hoàn thành mức demo |
| **Ngày 9** | Looker Studio ba trang, SQL validation, 5 business insights, ảnh/screenshots và GitHub | Hoàn thành |
| **Ngày 10** | Root README, Architecture, Data Dictionary, KPI, Docker templates, Demo Script, Git/security audit và cleanup có kiểm soát | **Hoàn thành / sign-off** |

## 4. Bằng chứng và các lần kiểm tra nổi bật

| Hạng mục | Bằng chứng đã cung cấp | Giới hạn diễn giải |
|---|---|---|
| dbt BigQuery | **11 models + 76 tests, 87/87 successful executions** | Thành công trong lần test ghi nhận; số model/tables có thể thay đổi nếu code phát triển tiếp |
| Airflow DAG | Batch có dữ liệu thành công; ví dụ **9 Success + 1 Skipped** do branch | `Skipped` ở nhánh không được chọn là hành vi kỳ vọng, không phải lỗi |
| DAG nhánh rỗng | Empty-batch branch được xác nhận | Không cần nạp dữ liệu khi không có batch mới |
| Watermark | QA-gated, commit 4 watermarks sau điều kiện chất lượng | Không tự coi cơ chế này là exactly-once cho mọi failure |
| Recovery | Retry drill **4/4 PASS** | Bài kiểm thử có kiểm soát |
| Partial write | Local filesystem/SQLite sandbox **8/8 PASS** | Không phải fault injection thực tế mọi nhánh GCS/BigQuery |
| GCS | Upload, file-level idempotency, `SKIP_VERIFIED`, checksum | Đã kiểm chứng trong demo |
| Sales reconciliation | **7 eligible orders** và **12.350.000 VND** ở cả `fct_orders` và `mart_city_day`; `orders_match=true`, `amount_match=true` | **[SNAPSHOT]** trong giai đoạn 01–19/10/2026 với cùng filter |
| Looker Studio | 3 trang: Sales Overview; City & Weather; Products & Items | Dữ liệu nhỏ/synthetic; weather association không chứng minh causation |
| Day 10 Docker | `local/airflow-gcp:3.3.2` và `local/airflow-gcp-dbt:3.3.2` có trên máy; hai Compose `config --quiet` trả mã 0 | Kiểm tra cấu hình trên máy hiện tại, **chưa thử cold start máy mới** |
| Day 10 Git | `README`, tài liệu, deployment examples và ảnh được Git theo dõi; nhánh `master...origin/master` sạch | Theo ảnh/đầu ra Terminal người thực hiện cung cấp |

**Ngày 10 — mốc Git cuối:** `09c110d` (`docs: add project demo script and interview Q&A`) được người thực hiện kiểm tra đang ở `HEAD -> master, origin/master` trước khi lập báo cáo này.

## 5. Tài liệu / artifact hiện có trong repository

| Đường dẫn | Nội dung |
|---|---|
| `README.md` | Trang giới thiệu root tiếng Anh cho GitHub |
| `docs/architecture.md` | Giải thích sơ đồ và checkpoint logic |
| `docs/architecture.png`, `.svg`, `.dot` | Sơ đồ kiến trúc và nguồn diagram |
| `docs/data_dictionary.md` | Grain, keys, fields và nguồn dữ liệu |
| `docs/kpi_definitions.md` | KPI logic, điều kiện lọc, hạn chế khi cộng gộp |
| `docs/day9_business_insights.md` | Năm insight và phân tích mô tả |
| `docs/day10_6_demo_script.md` | Kịch bản demo 7–8 phút và 10 câu hỏi phỏng vấn tiếng Anh |
| `deployment/airflow/README.md` | Hướng dẫn Docker/Compose reproducibility |
| `deployment/airflow/Dockerfile.*` | Các image Airflow/Google Provider/dbt với version pinning |
| `deployment/airflow/*.example.yaml`, `env.extra.example` | Compose / environment templates không chứa credential thật |
| `docs/screenshots/` | Bằng chứng Airflow, dbt, GCS/BigQuery, dashboard, Docker/Git |

**Một số ảnh tiêu biểu:**

- `docs/screenshots/day8_auto_pipeline_nonempty_success.png` (ảnh DAG nhánh có dữ liệu; giữ đúng tên đã được Git theo dõi).
- `docs/screenshots/day8_auto_pipeline_dbt_87_pass.png`.
- `docs/screenshots/day9_11_sales_overview.png`.
- `docs/screenshots/day9_city_weather_overview.png`.
- `docs/screenshots/day9_products_overview.png`.
- `docs/screenshots/day10_05_docker_compose_validation.png`.

**Không công khai:** `.env` thật, ADC/Service Account JSON, private `profiles.yml`, mật khẩu, tokens, dữ liệu nội bộ nhạy cảm. Các lệnh audit theo **tên file** đã đạt trong Ngày 10, nhưng không thay thế một lần quét secret toàn diện trên toàn bộ lịch sử Git.

## 6. Những sự cố quan trọng và bài học

1. **Chưa có GCS trong giai đoạn đầu:** Dùng Hugging Face Private RAW / PostgreSQL DEV để tiếp tục học; sau khi có billing GCP thì chuyển đích chính sang GCS + BigQuery. Giữ legacy để đối chiếu, không xóa hàng loạt.
2. **dbt/driver/version:** Cố định `dbt-core`, `dbt-bigquery` và kiểm tra profile/path thay vì chỉ nâng version không kiểm soát.
3. **Google Cloud và schema:** Phân biệt `fct_orders.total_amount` với `total_items_amount`, dùng `mart_city_day.total_order_amount`; SQL reconciliation xác nhận cùng tập đơn và giá trị.
4. **Airflow branching:** Task `Skipped` trên nhánh không được chọn có thể là kết quả thành công khi DAG đã hoàn tất.
5. **Checkpoint correctness:** Không commit watermark trước khi dữ liệu và QA được chấp thuận.
6. **Docker portability:** Docker Compose máy gốc chứa đường dẫn Windows/credential; tạo bản `deployment/airflow/` mẫu riêng thay vì commit bản gốc.
7. **Git LF/CRLF:** Windows cảnh báo đổi định dạng dòng không nhất thiết là lỗi; `git diff --cached --check` giúp phát hiện whitespace thực sự.
8. **File ảnh:** Sai khác `nonempty_success.png` và `nonempty_batch_success.png` làm `Test-Path=False`; giữ tên đã được Git theo dõi và cập nhật tài liệu tham chiếu.
9. **Repository cleanup:** Chỉ xóa hai thư mục `ecommerce_weather_dbt/tests/tests/` và `ecommerce_weather_dbt_bigquery/tests/tests/` sau khi xác nhận trống và không được Git theo dõi. `git clean -nd` sau đó không còn mục cần dọn theo phạm vi lệnh.
10. **Giữ bằng chứng:** Người thực hiện đã gộp một số ảnh Day 9; cách này hoàn toàn hợp lý nếu ảnh rõ, dễ đối chiếu và tên file khớp tài liệu.

## 7. Giới hạn chưa giải quyết — bắt buộc nói trung thực

| Nhóm | Tình trạng hiện tại | Không được tuyên bố quá mức |
|---|---|---|
| CDC | Timestamp-based incremental + soft-delete | Không phải production-grade WAL/Debezium CDC, chưa bao phủ mọi hard DELETE/late commit |
| Orchestration | Airflow transaction DAG `schedule=None`, manual-trigger | Không phải fully scheduled/unattended pipeline |
| Weather | Open-Meteo historical ingestion riêng | Không phải DAG giao dịch tự refresh weather mỗi lần |
| Delivery guarantees | Batch metadata, checksum, file-level replay safeguards, QA-gated watermark | Không đảm bảo universal exactly-once under all failures |
| Recovery | Drill/sandbox có kiểm soát | Chưa fault-inject toàn bộ cloud failure thật và concurrency |
| Data analysis | Demo/synthetic, cỡ mẫu nhỏ | Không suy luận causal effect weather → sales |
| Business metric | Eligible order amount & item line amount | Không mặc nhiên là recognized accounting revenue hoặc 2 metric luôn bằng nhau |
| Reproducibility | File mẫu và runbook, local Compose check thành công | Chưa độc lập triển khai fresh machine từ đầu |
| Operations | Chưa chứng minh CI/alerting/cost budgets đầy đủ | Không gọi là hệ thống production vận hành liên tục |
| Report sharing | Looker data sources có thể cần quyền BigQuery | Không mở public tùy tiện bằng Owner's credentials |

## 8. Repository cleanup và quyết định giữ file

- **Giữ:** `dags/`, `src/`, `sql/`, `ecommerce_weather_dbt_bigquery/`, `deployment/`, `docs/`, `data/`, `state/` vì có khả năng chứa thành phần hoạt động hoặc bằng chứng.
- **Giữ tạm làm legacy:** `ecommerce_weather_dbt/` (dbt-postgres), các `README_NGAY5_*`, `README_day8_*`, file sandbox/retry và ảnh bằng chứng.
- **Đã dọn:** Hai thư mục `tests/tests/` trống, không được Git theo dõi; không cần commit việc xóa.
- **Chưa dọn:** Các môi trường `.venv*`, cache/logs và workaround cũ; chỉ cân nhắc khi kiểm tra phụ thuộc và nhu cầu tiết kiệm dung lượng, không xóa vì trông “thừa”.
- **Trạng thái Git cuối theo ảnh:** `## master...origin/master` sạch, `git clean -nd` không liệt kê mục nào sau cleanup.

## 9. Các hướng nâng cấp tương lai (không bắt buộc cho Day 10)

### Ưu tiên P1 — để portfolio có tính tái lập tốt hơn

1. Kiểm thử triển khai từ máy/Windows profile mới bằng Docker templates; ghi lại những tiền điều kiện GCP IAM, bucket và profile.
2. Bổ sung secret scanning và CI cho `dbt parse`/unit checks, formatting, DAG import checks với secrets được cấp đúng cách.
3. Tối giản trang chủ GitHub, bảo đảm tất cả links và screenshots hoạt động; chọn dashboard share mode phù hợp.
4. Luyện demo tiếng Anh 7–8 phút và Q&A kỹ thuật; giữ ảnh bằng chứng sẵn để không cần trigger data mới.

### Ưu tiên P2 — nếu phát triển theo hướng production

1. CDC thông qua WAL/logical replication hoặc giải pháp tương đương; xử lý hard deletes, late-arriving updates, schema changes.
2. Scheduling an toàn, monitoring/alerts, SLA và budget/cost controls trước khi bật automation liên tục.
3. Fault injection cloud thật, concurrency/race conditions, idempotency end-to-end và data backfill/replay policies.
4. IAM least privilege, Secret Manager, phân quyền nguồn Looker Studio và audit logs.
5. Dữ liệu có quy mô và tính đại diện tốt hơn, thêm thống kê/causal methodology nếu muốn kiểm định quan hệ weather–sales.

## 10. Cách giới thiệu khi phỏng vấn Data Engineer

### 45-second English pitch (tham khảo)

> I built an end-to-end portfolio ELT pipeline combining PostgreSQL e-commerce transactions with historical weather data. I implemented Python timestamp-based incremental extraction, soft-delete handling, JSONL batches with metadata and checksums, GCS and BigQuery warehousing, and dbt transformations with automated quality checks. Airflow orchestrates the manually triggered transactional workflow and commits BigQuery watermarks only after downstream QA succeeds. I validated the demo with 87 successful dbt model/test executions, controlled retry and recovery tests, and three Looker Studio reporting pages. The dataset is synthetic and the pipeline is a portfolio demonstration, not a production WAL CDC system.

### Ba ý chính cho CV / README

- Designed and validated an incremental ELT pipeline using PostgreSQL, Python, GCS, BigQuery, dbt, Airflow, Docker, and Looker Studio.
- Implemented soft-delete-aware timestamp extraction, batch replay safeguards, and downstream QA-gated BigQuery watermark commits; verified 87/87 dbt model/test executions in recorded runs.
- Delivered analytical marts, SQL reconciliation checks, three dashboard pages, and documented recovery tests, architecture, and deployment templates.

**Không viết:** `fully scheduled production pipeline`, `WAL CDC`, `exactly-once under all failure modes`, `weather causally increased revenue` nếu chưa bổ sung bằng chứng.

## 11. Final sign-off checklist — ngày 20/11/2026

- [x] Demo source PostgreSQL, incremental & weather source đã được hoàn thành và ghi nhận.
- [x] GCS + BigQuery RAW/ETL/Model/Mart architecture và evidence đã được chuẩn bị.
- [x] dbt và Airflow các lượt demo/QA/recovery được người thực hiện xác nhận PASS.
- [x] BigQuery eligible order amount đối soát cùng mẫu date/status và ra `true`.
- [x] Looker Studio 3 trang và insights, ảnh bằng chứng Ngày 9 đã có.
- [x] Root README, architecture, dictionary, KPI, Docker templates đã được đưa lên GitHub.
- [x] Demo Script & Interview Q&A đã được đưa lên GitHub (`09c110d`).
- [x] Đã kiểm kê repository, dọn hai thư mục trống, không xóa file legacy quan trọng.
- [x] Đã xác nhận qua `git ls-files` các tài liệu và hai ảnh tiêu biểu hiện diện.
- [x] Lần kiểm tra cuối `git status -sb` sạch, `master...origin/master`.
- [ ] **Tùy chọn sau báo cáo:** Đưa file bàn giao này vào `docs/` và tạo commit tài liệu cuối cùng.

**Kết luận chính thức:** **DAY 10 SIGN-OFF — PASS (PORTFOLIO / DEMO SCOPE)**. Được phép ghi “Completed Personal Data Engineering Project”, nhưng phải giữ nguyên các caveat ở mục 7.

## 12. Khi tiếp tục ở cuộc trò chuyện sau

1. Cung cấp file bàn giao này hoặc liên kết repository GitHub; nói rõ phần muốn làm tiếp (CI, scheduling, CDC, portfolio, interview).
2. Không cần chạy lại Airflow/dbt/BigQuery chỉ để chứng minh các checkpoint đã có ảnh; chỉ rerun nếu có code/data thay đổi hoặc cần benchmark mới.
3. Trước khi chỉnh production-like behavior: kiểm tra chi phí cloud, IAM, trạng thái committed watermark và lịch đang bật.
4. Mọi thay đổi mới nên có commit riêng với kiểm tra `git diff --cached --check` và `git status -sb`.

---

**Nguồn nội bộ:** Bản tổng kết Ngày 1–9 ngày 17/11/2026, handbook dự án, ảnh/lệnh người thực hiện cung cấp trong quá trình Ngày 10, bao gồm đối soát BigQuery, Docker `config --quiet`, Git commit và repository cleanup.
