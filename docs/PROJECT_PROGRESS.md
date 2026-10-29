# PROJECT PROGRESS — E-commerce Sales & Weather Analytics

> **Tài liệu tiến độ phát triển dự án Data Engineering**  
> **Cập nhật:** 30/10/2026 (giờ Việt Nam)  
> **Phạm vi:** Công việc từ giai đoạn khởi tạo đến khi hoàn thành **Ngày 7**; kế hoạch cho Ngày 8–10.  
> **Trạng thái:** Đã xây dựng và kiểm thử luồng RAW → PostgreSQL DEV → dbt Staging/Intermediate/Marts; đã đưa mã nguồn lên GitHub **Private**. **Chưa có DAG Airflow end-to-end, BigQuery/GCS chính thức hay Looker Studio dashboard chính thức.**

## 1. Mục đích và cách đọc tài liệu

Tài liệu này lưu lại **quá trình thực hiện thực tế**, những kết quả đã được kiểm tra, các quyết định kỹ thuật, vấn đề từng gặp và những việc còn phải làm. Nó **không** phải lịch sử commit được tái tạo: repository Git được khởi tạo sau khi phần việc của những ngày đầu đã thực hiện. Không sửa ngày commit hoặc tạo commit lùi ngày để giả lập lịch sử phát triển.

**Mức độ bằng chứng:**

- **Đã kiểm tra bằng ảnh/log:** Có ảnh Terminal, màn hình pgAdmin/dbt Docs hoặc nội dung file được cung cấp và đối chiếu trong quá trình làm.
- **Đã được người thực hiện xác nhận:** Đã chạy thành công trên máy nhưng không có toàn bộ log/database hiện hành để kiểm toán độc lập.
- **Kế hoạch/chưa làm:** Chưa được xem là kết quả hoàn thành.

**Lưu ý về Ngày 1–4:** Handbook có phân chia mục tiêu theo ngày, nhưng tài liệu còn lưu không chứng minh chi tiết giờ/ngày thực tế mà từng công việc ban đầu được hoàn thành. Các mục “Ngày 1–4” dưới đây được **sắp xếp theo giai đoạn tương ứng trong roadmap**, không phải khẳng định đã có bốn commit hoặc bốn mốc nghiệm thu độc lập vào đúng từng ngày. **Ngày 5** có bản tổng kết ngày **26/10/2026**; **Ngày 6** có bản cập nhật đến **28/10/2026**; kết quả **Ngày 7** được xác nhận bằng các ảnh/log và trao đổi sau đó.

## 2. Tổng quan dự án

### 2.1. Bài toán nghiệp vụ

Xây dựng pipeline kết hợp **giao dịch thương mại điện tử** với **thời tiết lịch sử** để phân tích:

1. Số đơn và tổng giá trị đơn hợp lệ theo ngày, thành phố.
2. Sản phẩm/category, số lượng và giá trị dòng sản phẩm.
3. Sự khác biệt giữa những ngày có mưa và không mưa.
4. Tỷ lệ ghép được dữ liệu thời tiết vào đơn hàng.
5. Xu hướng/khác biệt có thể dùng để đưa ra giả thuyết nghiệp vụ.

Trong giai đoạn hiện tại, **`total_amount` của các đơn đủ điều kiện là chỉ số “tổng giá trị đơn hợp lệ”**, **không được tự gọi là doanh thu kế toán đã ghi nhận**, vì chưa có đối soát hoàn tiền, trả hàng, thanh toán và quy tắc ghi nhận doanh thu. Dữ liệu mẫu hiện nhỏ; **tương quan không chứng minh quan hệ nhân quả**.

### 2.2. Tech stack đã triển khai

| Nhóm | Thành phần | Vai trò thực tế |
|---|---|---|
| Transaction source | PostgreSQL | `customers`, `products`, `orders`, `order_items` |
| Ingestion | Python, `psycopg2` và các thư viện cần thiết | Incremental extract theo `updated_at`; JSONL và metadata |
| External source | Open-Meteo Historical Weather API | Trích xuất dữ liệu thời tiết theo thành phố/ngày |
| RAW cloud | Hugging Face **Private** Bucket | Lưu 8 file giao dịch JSONL + 3 file JSON weather trong snapshot đã kiểm tra |
| Warehouse DEV | PostgreSQL `ecommerce_analytics_dev` | Năm bảng `raw.*`, registry `etl.loaded_files`, các dbt views |
| Transformation | dbt-core **1.12.6**, dbt-postgres **1.11.0** | Staging, intermediate, marts, generic/singular tests, docs, lineage |
| Version control | Git + GitHub **Private** | Theo dõi source code và ảnh chứng minh sau khi khởi tạo Git |
| Hệ điều hành/phát triển | Windows, PowerShell, VS Code, pgAdmin | Môi trường triển khai local |

**Chưa triển khai:** Airflow/Cloud Composer, GCS/BigQuery theo yêu cầu cuối, Looker Studio/Looker, CI/CD và monitoring production.

### 2.3. Luồng xử lý đã hình thành

```text
PostgreSQL OLTP ---------------------> Python incremental extractor
 customers/products/orders/items       (updated_at + watermark)
                                                |
                                                v
                                           JSONL RAW
                                                |
Open-Meteo Historical API ----------> JSON weather RAW
                                                |
                                                v
                                  Hugging Face PRIVATE RAW
                                    8 JSONL + 3 JSON (snapshot)
                                                |
                                  Python RAW loader / SHA-256
                                                |
                                                v
                                PostgreSQL analytics DEV
                              raw.* + etl.loaded_files
                                                |
                                      dbt source + staging
                                     (5 staging views)
                                                |
                                +---------------+---------------+
                                |                               |
                         dbt intermediate                  dbt marts
                         (3 models)                      (3 models)
                                +---------------+---------------+
                                                |
                                        dbt tests / docs
                                                |
                                    dashboard: CHƯA LÀM

Airflow orchestration: CHƯA LÀM
BigQuery/GCS chính thức: CHƯA LÀM
```

> Đây là sơ đồ **logic** của các công đoạn đã có; **chưa có scheduler/DAG tự động điều phối toàn bộ**. Các model được sắp trong thư mục dbt theo từng lớp; trong kết quả đã kiểm tra, chúng đang được materialize dưới schema PostgreSQL **`staging`**, chưa có bằng chứng tách thành các schema vật lý riêng cho intermediate/marts.

## 3. Nhật ký phát triển theo giai đoạn

### Ngày 1 — Phân tích bài toán và định hướng kiến trúc

**Mục tiêu theo handbook:** Chốt business problem, câu hỏi phân tích, kiến trúc v1, source schema.

**Kết quả có thể xác nhận ở thời điểm hiện tại:**

- Đã chọn chủ đề **E-commerce Sales & Weather Analytics**.
- Đã xác định hai nhóm dữ liệu: giao dịch PostgreSQL và thời tiết qua API.
- Đã chọn luồng lưu RAW trước khi transform, rồi xây Warehouse, dbt và dashboard.
- Đã có thiết kế bốn bảng nguồn và định hướng sử dụng `updated_at`/soft delete cho thay đổi dữ liệu.
- Đã xác định các câu hỏi về số đơn, thành phố, sản phẩm và thời tiết.

**Bằng chứng hiện có:** Handbook, bản tổng kết Ngày 5 và các SQL/schema đã lưu. **Chưa có chứng cứ tách riêng rằng mọi đầu việc này hoàn tất đúng trong ngày lịch đầu tiên.**

### Ngày 2 — Tạo PostgreSQL source và dữ liệu mẫu

**Mục tiêu theo handbook:** Dựng database nguồn, schema, dữ liệu ban đầu.

**Đã có:**

| Bảng nguồn | Các trường nghiệp vụ chính |
|---|---|
| `customers` | `customer_id`, `customer_name`, `city`, `created_at`, `updated_at` |
| `products` | `product_id`, `product_name`, `category`, `price`, timestamps |
| `orders` | `order_id`, `customer_id`, `order_date`, `status`, `city`, `total_amount`, `is_deleted`, timestamps |
| `order_items` | `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`, timestamps |

- Có SQL tạo schema và dữ liệu ban đầu: [`sql/source_schema.sql`](../sql/source_schema.sql), [`sql/seed_data.sql`](../sql/seed_data.sql).
- Có quan hệ khóa nghiệp vụ giữa đơn hàng, khách hàng, sản phẩm và dòng sản phẩm.
- Có trigger cập nhật `updated_at` khi UPDATE.
- Trạng thái đơn hàng được hỗ trợ: `pending`, `paid`, `shipped`, `delivered`, `cancelled`.

**Giới hạn nguồn:** Bản tổng kết xác nhận những thành phần đã có **trước Ngày 5**, không xác nhận độc lập số bản ghi seed ban đầu hay thời điểm chính xác của từng lần chạy trong Ngày 2.

### Ngày 3 — Mô phỏng thay đổi dữ liệu và Incremental Extraction

**Mục tiêu theo handbook:** INSERT/UPDATE/DELETE (mô phỏng), incremental extraction, watermark.

**Đã xây dựng:**

- Script [`sql/day3_simulate_changes.sql`](../sql/day3_simulate_changes.sql) để mô phỏng thay đổi dữ liệu nguồn.
- Script [`src/ingestion/extract_incremental.py`](../src/ingestion/extract_incremental.py) đọc các bảng `customers`, `products`, `orders`, `order_items`.
- Watermark cục bộ: `state/watermark.json` (file trạng thái vận hành, không đưa lên Git).
- Mỗi lần chạy lấy khoảng `updated_at > last_watermark AND updated_at <= current_watermark`, có thứ tự theo timestamp và khóa chính.
- Dữ liệu được ghi thành JSONL tại `data/raw/transactions/<table>/dt=<date>/...`.
- Các trường truy vết trong JSONL: `_source_table`, `_extracted_at`, `_watermark_from`, `_watermark_to`.
- Có mô phỏng xóa mềm thông qua `orders.is_deleted`; không xóa mất dấu vết khỏi tầng RAW.

**Giới hạn phải giữ trung thực:** Đây là **timestamp-based incremental extraction**, **không phải WAL-based CDC**; không tự phát hiện hard DELETE. Trong bản code đã xem, watermark được cập nhật sau khi trích xuất local hoàn tất, **chưa chờ upload cloud và nạp warehouse thành công**. Do đó cần thiết kế lại checkpoint/khả năng replay trước khi tự động hóa Airflow.

**Trạng thái:** Logic và script đã có/được thực hành; không có bằng chứng đủ để chứng minh mọi trường hợp lỗi, late commits hoặc replay toàn pipeline đã được kiểm thử từ Ngày 3.

### Ngày 4 — Weather API và RAW storage

**Mục tiêu theo handbook:** External ingestion + lưu RAW, mục tiêu gốc có GCS.

**Đã triển khai trong môi trường thực hành:**

- Tích hợp **Open-Meteo Historical Weather API**.
- Các script liên quan: [`src/ingestion/extract_weather.py`](../src/ingestion/extract_weather.py), [`src/transformation/transform_weather.py`](../src/transformation/transform_weather.py), [`src/loading/load_weather.py`](../src/loading/load_weather.py).
- Bảo lưu phản hồi weather ở dạng JSON với `metadata` và `response`; bản chuẩn hóa local là `data/processed/weather/weather_daily.json`.
- Snapshot weather đã được ghi nhận: **Hanoi 19 ngày, Da Nang 5 ngày, Ho Chi Minh City 4 ngày = 28 cặp city-date**.
- Đã có một báo cáo/ghép dữ liệu và Excel/CSV **pilot**; **chưa phải** mart dbt hoặc dashboard chính thức.
- Chọn **Hugging Face Private Bucket** cho RAW trong giai đoạn DEV thay vì GCS do GCP chưa sẵn sàng.

**Phân biệt mốc:** Việc **đồng bộ đủ 8 file JSONL giao dịch lên bucket**, đối soát cloud và nạp xuống warehouse được ghi nhận cụ thể trong **Ngày 5**. Không ghi nhầm các bước đó là đã nghiệm thu đầy đủ ở Ngày 4.

### Ngày 5 — Đồng bộ RAW cloud và nạp PostgreSQL Warehouse DEV

**Ngày tổng kết có ghi nhận:** **26/10/2026**.

**Đã thực hiện:**

1. Dùng `hf buckets sync` để đưa **8 file JSONL giao dịch** lên Hugging Face Private Bucket; cộng **3 file JSON weather** đã có, tổng cộng **11 file** trong snapshot.
2. Tải mẫu **một file RAW** về và so SHA-256 với local: kết quả **khớp (`True`)**. Chưa đối chiếu SHA-256 thủ công tất cả 11 file.
3. Tạo PostgreSQL analytics database tách biệt nguồn: **`ecommerce_analytics_dev`**.
4. Tạo `raw.customers`, `raw.products`, `raw.orders`, `raw.order_items`, `raw.weather_api_responses` và registry `etl.loaded_files`.
5. Viết/chạy [`sql/day5_create_raw_tables.sql`](../sql/day5_create_raw_tables.sql) và [`src/loading/load_raw_to_postgres.py`](../src/loading/load_raw_to_postgres.py).
6. Loader download RAW vào thư mục tạm, tính hash SHA-256, đối chiếu registry và ghi RAW + registry trong cùng transaction của từng file.
7. File cùng đường dẫn/checksum: **skip**. File có cùng đường dẫn nhưng checksum đổi: **báo lỗi** để kiểm tra.

**Kết quả nạp lần đầu (snapshot đã kiểm tra):**

| Bảng | Số dòng RAW |
|---|---:|
| `raw.customers` | 5 |
| `raw.products` | 6 |
| `raw.orders` | 11 |
| `raw.order_items` | 16 |
| `raw.weather_api_responses` | 3 |
| **Tổng** | **41** |

- **38 dòng giao dịch JSONL** + **3 dòng weather JSONB** = **41 RAW rows**.
- RAW `orders` có thể chứa nhiều phiên bản của cùng `order_id` (giữ lịch sử extract); RAW weather **3 JSON documents**, không phải 28 bản ghi ngày.
- Registry ghi nhận **11 file** được nạp.
- Chạy loader lần hai: **0 file mới, 11 file bỏ qua, 0 dòng mới**, số lượng RAW không tăng — **idempotency ở cấp file đã được xác nhận trên snapshot này**.
- Query mảng ngày trong JSONB tiếp tục cho **28 cặp city-date**.

**Quyết định kỹ thuật:** GCP/BigQuery được **tạm hoãn**, thay bằng PostgreSQL Warehouse DEV để tiếp tục các phần dbt và kiểm chứng SQL; không được coi đây là hoàn thành yêu cầu BigQuery.

### Ngày 6 — dbt Staging, Data Quality và Git local

**Mốc tổng kết đã có:** Đến **28/10/2026**.

**Môi trường:**

- Tạo project `ecommerce_weather_dbt` và dùng `dbt-postgres` kết nối **warehouse DEV**, không kết nối nhầm source DB.
- Cấu hình `profiles.yml` lấy credentials từ `DBT_PG_USER` và `DBT_ENV_SECRET_PG_PASSWORD`.
- Cố định `dbt-core==1.12.6` và `dbt-postgres==1.11.0` trong `requirements-dbt.txt`; tạo `requirements-dbt.lock.txt` theo snapshot môi trường.
- Dùng `.venv_dbt_stable` để chạy dbt; `pip check` không phát hiện gói phụ thuộc lỗi.
- `dbt debug`, `dbt parse`, `dbt build` được xác nhận thành công sau khi xử lý cấu hình ví dụ trong `models/example`.

**5 staging models:**

| Model | Công việc chính |
|---|---|
| `stg_customers` | Chuẩn hóa ID/thông tin khách và chọn latest-state theo business key |
| `stg_products` | Chuẩn hóa sản phẩm/giá, chọn latest-state |
| `stg_orders` | Chuẩn hóa status/city, giữ `is_deleted`, chọn latest-state theo `order_id` |
| `stg_order_items` | Chuẩn hóa khóa item/order/product, chọn latest-state theo `order_item_id` |
| `stg_weather_daily` | Tách các mảng `daily` trong JSONB thành bản ghi city-date và deduplicate |

- Các bảng giao dịch dùng `ROW_NUMBER()` kết hợp `updated_at`, `_extracted_at`, `loaded_at`, `raw_event_id` để chọn dòng mới nhất.
- Source definitions: `models/staging/sources.yml`; mô tả/tests: `models/staging/staging.yml`.
- Thêm generic tests như `not_null`, `unique`, `accepted_values`; singular tests kiểm tra số tiền, giá trị item và khóa weather.
- Đã xem dbt docs/lineage và đối chiếu kết quả staging.
- Snapshot từng kiểm tra: **RAW orders 11**, **distinct orders 9**, **`stg_orders` 9**, trong đó **1 đơn soft-deleted**.

**Git được khởi tạo ở giai đoạn này (không phải Ngày 1):**

- `git init`, thiết lập `.gitignore` để loại trừ `.env`, `data/`, virtualenv, dbt build artifacts và các file vận hành.
- Commit đầu tiên bao gồm **35 file** (theo báo cáo tại thời điểm tạo commit), message `Initial commit: ecommerce and weather data engineering pipeline`.
- **Mã `3afb0a1` là mã commit ban đầu trước khi sửa email tác giả**, **không nên dùng làm mã commit hiện hành**. Khi giải quyết GH007, commit đã được `--amend` và hash có thể đã thay đổi; hash hiện hành phải đọc bằng `git log`.
- Việc **push thành công lên GitHub Private diễn ra sau các bước kiểm tra bảo mật và sửa email**, trước khi triển khai Ngày 7; không suy diễn rằng có commit riêng của Ngày 1–5.

### Ngày 7 — dbt Intermediate, Fact, Mart, Tests, Documentation và GitHub

**Mốc bằng chứng:** Phiên thực hiện sau bản tổng kết 28/10; ảnh Terminal/Docs/GitHub và xác nhận của người thực hiện trong ngày làm việc tiếp theo. Không lấy mốc từ commit date giả định.

#### 7.1. Kết nối GitHub và xử lý bảo mật trước khi phát triển tiếp

- Kiểm tra `git status`, `git log`, `git remote`, danh sách 35 file trong commit.
- Quét tìm dấu hiệu password/secret/token trong file đã commit; kiểm tra ba Python scripts `extract_incremental.py`, `load_raw_to_postgres.py`, `load_weather.py` không thấy mật khẩu/token thật hardcode trong nội dung đã cung cấp.
- Xác nhận file vận hành/secrets và raw data được `.gitignore` loại trừ trong phạm vi kiểm tra.
- Tạo GitHub repository **Private** và remote `origin`.
- Lần push đầu bị GitHub **GH007** (email tác giả riêng tư). Cập nhật Git email `noreply` ở repo local, amend commit đầu tiên để sửa Author/Committer, rồi push thành công theo xác nhận người thực hiện.
- **Không tắt chính sách bảo vệ email; không tạo lịch sử commit lùi ngày.**

> Đây là kiểm tra bảo mật **sơ bộ** đối với những file được chia sẻ và danh sách Git, không phải kiểm toán bảo mật độc lập toàn repo hay hạ tầng cloud.

#### 7.2. Intermediate models (3 models)

| File | Logic đã xây dựng |
|---|---|
| `models/intermediate/int_orders_eligible.sql` | Lọc status `paid`/`shipped`/`delivered`, yêu cầu `is_deleted IS FALSE`; giữ 1 dòng/order từ staging; tính `order_datetime_vn` và `order_date_vn` |
| `models/intermediate/int_order_items_by_order.sql` | `GROUP BY order_id`: `item_count`, `distinct_product_count`, `total_quantity`, `total_items_amount` |
| `models/intermediate/int_orders_weather.sql` | `LEFT JOIN` fact orders với weather theo city + ngày Việt Nam, giữ đơn thiếu weather; phân loại `rainy`/`dry`/`unknown` |

- Kiểu `order_date` đã được kiểm tra là **`timestamp with time zone`** trong PostgreSQL.
- Ngày địa phương được tạo bằng `(order_date AT TIME ZONE 'Asia/Ho_Chi_Minh')::date` **trước** khi ghép `weather_date`.
- Khóa city hiện chuẩn hóa cơ bản bằng `LOWER(BTRIM(...))`; chưa có bảng mapping xử lý mọi biến thể tên như `Hanoi` và `Ha Noi`.

#### 7.3. Fact & Mart models (3 models)

| File | Grain | Logic/KPI |
|---|---|---|
| `models/marts/fct_orders.sql` | 1 dòng / `order_id` hợp lệ | `LEFT JOIN` orders với tổng hợp items; giữ `total_amount` của orders; `has_order_items` cho trường hợp không có items |
| `models/marts/fct_order_items.sql` | 1 dòng / `order_item_id` thuộc đơn hợp lệ | Join `stg_order_items` với `fct_orders`; `line_amount = quantity * unit_price`; không nhân `orders.total_amount` lên từng item |
| `models/marts/mart_city_day.sql` | 1 dòng / normalized city + report date | Nhóm fact orders theo city/ngày rồi `FULL OUTER JOIN` với daily weather; các KPI `order_count`, `total_order_amount`, AOV, số đơn theo status, thời tiết |

**Quy tắc của mart:**

- Dùng **`orders.city` là thành phố giao hàng**, không tự thay bằng `customers.city`.
- Ngày có weather nhưng không có orders vẫn xuất hiện: `order_count=0`, `total_order_amount=0`, `avg_order_value=NULL`.
- Ngày có orders nhưng không ghép được weather vẫn giữ orders, còn chỉ số weather là `NULL`, `weather_condition='unknown'`.
- Tránh JOIN trực tiếp một order với nhiều `order_items` rồi cộng lặp `total_amount`.
- Phân biệt `total_items_amount` (tổng dòng item) và `total_amount` (giá trị đơn nguồn); hai giá trị **không mặc nhiên phải bằng nhau** nếu có chính sách phí/giảm giá.

#### 7.4. Tests, reconciliation và xử lý lỗi

- Tạo/cập nhật `intermediate.yml`, `marts.yml` cùng generic tests cho unique/not-null/accepted-values/relationships.
- Thêm singular tests, trong đó có:
  - `assert_int_orders_eligible_rules.sql` — không lọt đơn sai status hoặc soft-deleted.
  - `assert_fct_orders_reconciliation.sql` — giữ đúng đơn và `total_amount` so với intermediate.
  - `assert_weather_city_day_normalized_unique.sql` — tránh trùng city-date sau chuẩn hóa đơn giản.
  - `assert_mart_city_day_unique.sql` — unique grain thành phố–ngày.
  - `assert_mart_city_day_reconciliation.sql` — tổng orders/tổng amount khớp fact.
  - `assert_mart_weather_coverage.sql` — không mất những ngày có weather.
- Đã kiểm tra các truy vấn `dbt show`, kết quả trong pgAdmin và quan hệ đơn hàng/thời tiết theo trao đổi.
- Kết quả `fct_orders` từng hiển thị **7 đơn đủ điều kiện** (`O001`, `O002`, `O003`, `O004`, `O006`, `O007`, `O008`) tại một snapshot; **không biến số 7 thành ràng buộc vĩnh viễn**.
- Trong quá trình build từng phạm vi, từng gặp **2 lỗi test relationships** do view `fct_order_items`, `int_orders_weather` chưa tồn tại khi test chạy; sau đó đã tạo đủ view.
- Một lần full build tiếp theo báo lỗi tạo `stg_orders` và skip các tests phụ thuộc; **chưa xác định chắc chắn root cause** từ ảnh lỗi đó. Lần chạy lại cuối đã thành công, không còn ERROR/SKIP. Không ghi nguyên nhân chưa được chứng minh thành sự thật.

**Bằng chứng kiểm thử toàn project mới nhất:**

```text
Finished running 76 data tests, 11 view models in 9.40s.
Completed successfully
Done. PASS=87 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=87
```

> **87 PASS là 87 bước tổng cộng, gồm 11 views + 76 data tests**, **không phải 87 tests**. Kết quả này chứng minh project dbt đã build và qua các tests hiện có trên môi trường DEV, **không phải chứng nhận production**.

#### 7.5. dbt Documentation, Lineage và Git

- Đã chạy/giao diện hiển thị `dbt docs`, xem trang chi tiết `mart_city_day` với `view`, relation **`ecommerce_analytics_dev.staging.mart_city_day`**, description, columns và tests.
- Đã kiểm tra Data Lineage từ RAW → Staging → Intermediate/Fact/Mart.
- Đường dẫn cha trực tiếp của `mart_city_day` là **`fct_orders`** và **`stg_weather_daily`**; model `int_orders_weather` là nhánh riêng, **không** phải parent trực tiếp của mart này.
- Đã làm rõ bộ lọc graph `+mart_city_day+` không hiển thị hai nhánh `fct_order_items` và `int_orders_weather`; đổi sang `+fct_orders+` hoặc chọn riêng từng nhánh để xác minh.
- Đã lưu hai ảnh ở `docs/screenshots/` trên máy/repository:
  - [`dbt_lineage.png`](screenshots/dbt_lineage.png)
  - [`mart_city_day_docs.png`](screenshots/mart_city_day_docs.png)
- Đã stage/commit/push các models, tests và screenshots Ngày 7 lên GitHub **Private**, sau đó người thực hiện xác nhận tất cả thành công.

**Commit messages đã sử dụng:**

1. `Initial commit: ecommerce and weather data engineering pipeline` — phiên bản code có sẵn đến Ngày 6; commit được **amend** để bảo vệ email.
2. `feat(dbt): add intermediate models and analytics marts` — phần xây dựng Ngày 7.

**Không tự điền hash hai commit hiện hành** vì chưa thu thập `git log -2 --oneline` sau push. Khi cần ghi chính xác hash/commit date, hãy lấy trực tiếp từ repository Git hiện tại.

## 4. Kết quả tổng hợp đến hết Ngày 7

| Hạng mục | Mức độ hoàn thành | Bằng chứng/ghi chú |
|---|---|---|
| PostgreSQL source schema và dữ liệu mẫu | Đã thực hiện | SQL scripts + các bản tổng kết |
| Batch incremental theo timestamp + soft delete | Đã thực hiện | Python source; cần nâng cấp checkpoint/replay |
| Open-Meteo weather ingestion | Đã thực hiện | Snapshot 28 city-date |
| Hugging Face Private RAW | Đã thực hiện | 11 files trong snapshot Ngày 5 |
| PostgreSQL analytics DEV + RAW registry | Đã thực hiện | 5 RAW tables + `etl.loaded_files` |
| Loader idempotent theo file/checksum | Đã kiểm tra snapshot | Lần 2: 0 nạp mới, 11 skip |
| 5 dbt staging models | Đã thực hiện | Build/tests và code đã xem |
| 3 dbt intermediate models | Đã thực hiện | Build + tests toàn project |
| 3 dbt mart models | Đã thực hiện | `fct_orders`, `fct_order_items`, `mart_city_day` |
| dbt tests + reconciliation | Đã kiểm thử | **76 data tests PASS** trong full build gần nhất |
| dbt views toàn project | Đã build | **11 views**; tổng 87 PASS |
| dbt docs & lineage | Đã xem và chụp ảnh | `docs/screenshots/` |
| GitHub Private (source Day 6–7) | Đã push, người thực hiện xác nhận | Không suy ra commit lịch sử Ngày 1–5 |
| Airflow orchestration | **Chưa làm** | Tiếp theo Ngày 8 |
| GCS / BigQuery | **Chưa làm** | PostgreSQL + Hugging Face là phương án DEV |
| Looker Studio dashboard | **Chưa làm** | Excel/CSV trước đây chỉ là pilot |
| README gốc hoàn chỉnh, demo và insight cuối | **Chưa hoàn thiện** | Ngày 9–10 |

## 5. Những quyết định và giới hạn kỹ thuật quan trọng

### 5.1. Incremental ≠ CDC dựa trên log

`updated_at` + watermark mô phỏng dòng thay đổi và soft delete, nhưng không tự bắt hard DELETE, không đảm bảo bắt mọi transaction commit muộn, và có thể cần lookback window/mốc checkpoint an toàn. Không mô tả là CDC WAL/Datastream khi chưa triển khai.

### 5.2. Checkpoint phải an toàn với lỗi giữa các công đoạn

Trong `extract_incremental.py` đã xem, `save_watermarks()` chạy **sau local extract thành công**, nhưng **trước** các script upload/load tiếp theo. Khi Airflow retry mà upload thất bại, có nguy cơ một lần extract kế tiếp bỏ qua các sự kiện ở khoảng thời gian đã watermark. Cần chọn chiến lược **batch ID + manifest + upload/load xác nhận + checkpoint/commit sau cùng** hoặc bảo đảm replay an toàn, rồi test failure injection trước khi khẳng định end-to-end idempotency.

### 5.3. RAW và current-state khác nhau

RAW lưu các sự kiện/snapshot extract; staging dùng `ROW_NUMBER()` để chọn latest-state theo business key. Snapshot `raw.orders=11`, `stg_orders=9` **không phải lỗi mất hai đơn**. `is_deleted` được giữ ở RAW/staging và loại có chủ đích khi tính KPI.

### 5.4. Grain, đơn hợp lệ, JOIN weather và timezone

- `fct_orders`: 1 dòng/eligible order.
- `fct_order_items`: 1 dòng/eligible order item.
- `mart_city_day`: 1 dòng/`normalized city + report_date`.
- Eligible status: `paid`, `shipped`, `delivered`, `is_deleted IS FALSE`.
- `order_date` là `timestamptz`, chuyển về `Asia/Ho_Chi_Minh` trước khi lấy ngày.
- Weather JOIN theo **shipping city `orders.city`** + **local order date**, không phải customer city.
- Thiếu weather => `unknown`, **không** tự coi là `dry`.
- `LOWER(BTRIM(city))` là chuẩn hóa giới hạn; chưa có mapping tên thành phố đầy đủ.

### 5.5. Vấn đề thống kê và diễn giải

- Mẫu 28 city-date không đồng đều giữa ba thành phố.
- 7 đơn đủ điều kiện chỉ là **snapshot quan sát**, không phải con số ràng buộc để kiểm thử với dữ liệu mới.
- Với mẫu nhỏ, không đưa ra kết luận chắc chắn rằng mưa **gây ra** thay đổi giá trị đơn.
- Khi mở rộng dataset, cần kiểm tra coverage theo khoảng ngày/địa điểm và nguồn sai khác chính sách tính tiền.

### 5.6. Security và Git

- Không commit `.env`, credentials, HF token, file RAW/dữ liệu nhạy cảm, `.venv*`, file `target/`, `logs/` hoặc môi trường runtime.
- `.gitignore` **không loại bỏ secrets đã có trong commit**; phải rotate/revoke và xử lý lịch sử nếu phát hiện.
- GitHub có chế độ **Private**; quyết định mở Public chỉ thực hiện khi documentation, kiểm tra dữ liệu và bảo mật đã hoàn thiện.
- Git history thực tế chỉ bắt đầu ở Ngày 6; tài liệu này giải thích tiến độ trước đó, **không thay thế commit history**.

### 5.7. Warehouse DEV chưa đạt yêu cầu đích của handbook

Handbook yêu cầu sử dụng GCP để lưu trữ/xử lý và có **BigQuery table design**. Phương án hiện tại **Hugging Face Private + PostgreSQL DEV** giúp xây và kiểm thử logic, nhưng **không đủ để khẳng định yêu cầu GCP/BigQuery đã hoàn thành**. Cần lập kế hoạch chuyển/nhân bản sang BigQuery, xử lý SQL dialect, partition/clustering, chi phí và quyền truy cập. Dù mart đã sẵn sàng, dashboard Looker Studio chính thức vẫn chưa được xác nhận.

## 6. Việc còn phải làm — Lộ trình sau Ngày 7

### Ngày 8 — Apache Airflow Orchestration (ưu tiên ngay)

- [ ] Kiểm tra RAM/disk, môi trường Windows, WSL2/Docker và chọn cách chạy Airflow hợp lý.
- [ ] Xác minh các scripts có thể chạy không tương tác, quản lý credentials qua env/connections/secrets.
- [ ] **Ưu tiên trước orchestration:** thiết kế checkpoint/manifest để xử lý lỗi upload/load sau local extraction; không update watermark sớm.
- [ ] Tạo DAG phụ thuộc: `extract_transactions` → `upload_raw` → `load_raw` → `dbt_build` → `data_quality`; external weather ingestion cần nhánh tương ứng và điều kiện đồng bộ.
- [ ] Thêm timeout/retry/retry delay, idempotency, log theo run ID, kiểm tra trạng thái task.
- [ ] Chạy 2 lần liên tiếp, xác minh không nạp duplicate; thêm thử nghiệm cố ý cho một bước fail rồi recovery.
- [ ] Commit code theo thay đổi chức năng và push GitHub Private.

**Tiêu chí thành công:** Một DAG có thể chạy end-to-end, logs rõ, retry không làm mất dữ liệu hoặc nhân bản bản ghi; chỉ đánh dấu hoàn thành khi có log và kiểm tra database tương ứng.

### Ngày 9 — BigQuery/GCP và Dashboard

- [ ] Có GCP project, quyền và billing/cost control phù hợp.
- [ ] Thiết kế BigQuery datasets/table design, partition/clustering theo nhu cầu truy vấn.
- [ ] Đưa RAW/warehouse lên kiến trúc GCP đã chọn (nếu dùng GCS, không nhầm HF private URI với `gs://`).
- [ ] Tương thích hóa loader/SQL và chuyển hoặc bổ sung `dbt-bigquery`; chạy lại tests và reconciliation trước khi dùng số liệu.
- [ ] Tạo dashboard Looker Studio/Looker từ mart với ngày, thành phố, số đơn, tổng giá trị đơn hợp lệ, AOV, lượng mưa và nhiệt độ.
- [ ] Ghi 3–5 insight có dẫn số liệu, nêu rõ kích thước mẫu/giới hạn và không suy diễn quan hệ nhân quả.

**Nếu GCP chưa sẵn sàng:** Có thể tiếp tục demo DEV, nhưng vẫn để BigQuery/Looker ở trạng thái **chưa hoàn thành**, không ghi là đã triển khai production/GCP.

### Ngày 10 — Portfolio, tài liệu và demo

- [ ] Tạo README gốc đầy đủ: vấn đề, data sources, architecture, công nghệ, setup, lệnh chạy, KPI definitions, kết quả và limitations.
- [ ] Hoàn thiện architecture diagram, data dictionary, screenshots, proof of test, demo flow.
- [ ] Kiểm thử setup sạch từ dependencies và `.env.example` chỉ có tên biến/placeholder, không có giá trị thật.
- [ ] Tài liệu hóa Git/GitHub, cách retry/replay, xử lý soft delete và watermark.
- [ ] Quay/chạy demo 5–10 phút: source change → RAW → transform → mart → dashboard + insight.
- [ ] Kiểm tra secrets/data privacy và quyết định có chuyển repo Public không.

## 7. So sánh với những đầu ra chính trong handbook

| Đầu ra handbook | Trạng thái đến hết Ngày 7 |
|---|---|
| GitHub repository chứa source code | **Đã có GitHub Private** (theo xác nhận) |
| README project đầy đủ | **Chưa hoàn thiện** |
| Architecture diagram | Có mô tả kiến trúc; **sơ đồ portfolio chuẩn cần hoàn thiện** |
| Source database schema | **Đã có** |
| Script generate/ingest dữ liệu | **Đã có** |
| External API/crawler ingestion | **Đã có Open-Meteo** |
| Airflow/Composer orchestration | **Chưa có** |
| dbt project | **Đã có** 11 views/76 tests trong lần build đạt |
| BigQuery table design | **Chưa hoàn thiện** |
| Dashboard link/screenshot | **Chưa có dashboard chính thức** |
| Báo cáo insights cuối | **Chưa hoàn thiện** |
| Demo 5–10 phút | **Chưa hoàn thiện** |

## 8. File và bằng chứng đáng chú ý

### 8.1. Các file nguồn chính

```text
data-engineering-project/
├── .gitignore
├── requirements.txt
├── requirements-dbt.txt
├── requirements-dbt.lock.txt
├── sql/
│   ├── source_schema.sql
│   ├── seed_data.sql
│   ├── day3_simulate_changes.sql
│   └── day5_create_raw_tables.sql
├── src/
│   ├── ingestion/
│   │   ├── extract_incremental.py
│   │   └── extract_weather.py
│   ├── transformation/
│   │   └── transform_weather.py
│   └── loading/
│       ├── load_raw_to_postgres.py
│       └── load_weather.py
├── ecommerce_weather_dbt/
│   ├── models/
│   │   ├── staging/         # 5 views
│   │   ├── intermediate/    # 3 views
│   │   └── marts/           # 3 views
│   └── tests/               # singular tests
└── docs/
    ├── PROJECT_PROGRESS.md
    └── screenshots/
        ├── dbt_lineage.png
        └── mart_city_day_docs.png
```

> Đây là **cấu trúc rút gọn** dựa trên các file đã được chia sẻ và lịch sử thực hiện, không phải bản dump đầy đủ mọi file trong repository. `state/watermark.json`, `data/`, `.env` và virtualenv là file/thư mục runtime local nên không liệt kê như file Git.

### 8.2. Ảnh minh chứng

- [dbt Lineage Graph](screenshots/dbt_lineage.png) — ảnh DAG nhánh RAW → staging → intermediate → mart.
- [dbt Documentation: mart_city_day](screenshots/mart_city_day_docs.png) — ảnh mô tả model, relation, các cột và tests.
- Log `dbt build`: **87 PASS, 0 WARN, 0 ERROR, 0 SKIP** (ảnh Terminal đã được người thực hiện chia sẻ trong quá trình xây dựng).
- Log initial RAW loader và rerun: **11 file/41 bản ghi lần đầu; 0 new/11 skipped lần 2** theo tổng kết Ngày 5.

## 9. Lệnh kiểm tra và cập nhật tài liệu

### Kiểm tra Git từ thư mục gốc repository

```powershell
git status
git log -2 --oneline
git remote -v
```

### Chạy dbt từ thư mục `ecommerce_weather_dbt`

```powershell
..\.venv_dbt_stable\Scripts\dbt.exe --version
..\.venv_dbt_stable\Scripts\dbt.exe debug
..\.venv_dbt_stable\Scripts\dbt.exe build
..\.venv_dbt_stable\Scripts\dbt.exe docs generate
..\.venv_dbt_stable\Scripts\dbt.exe docs serve --port 8080
```

> Cần nạp an toàn các biến môi trường như `DBT_PG_USER` và `DBT_ENV_SECRET_PG_PASSWORD` trong phiên PowerShell mới. **Không đưa mật khẩu/token vào screenshot, log công khai, commit hoặc tài liệu này.**

### Đưa tài liệu này vào GitHub

Sau khi lưu file tại **`docs/PROJECT_PROGRESS.md`** trong project local:

```powershell
# Chạy ở thư mục gốc data-engineering-project
git add docs/PROJECT_PROGRESS.md
git diff --cached --check
git diff --cached --name-only
git commit -m "docs: add project progress through day 7"
git push origin master
git status
```

Lệnh `git status` cuối nên báo **working tree clean**. Nếu có file mới ngoài dự kiến hoặc chứa thông tin nhạy cảm, rà soát trước khi commit.

---

## 10. Quy tắc cập nhật cho các buổi sau

Mỗi khi hoàn thành một mốc chức năng, bổ sung:

1. **Ngày/phiên làm việc** và mục tiêu.
2. **Files đã thêm/sửa** (đường dẫn và vai trò).
3. **Bằng chứng test** (lệnh, PASS/FAIL, row counts, screenshots không chứa secrets).
4. **Vấn đề gặp phải, cách xử lý và điều chưa biết** (không suy đoán root cause).
5. **Commit hash thực tế** lấy từ `git log` sau khi commit/push.
6. **Việc chưa làm và bước tiếp theo**.

Từ Ngày 8 trở đi, ưu tiên các commit nhỏ theo chức năng, không cần một commit/ngày và **không tạo lịch sử Git giả cho các giai đoạn trước Ngày 6**.

### Nguồn biên soạn

- *Data Engineer Project Handbook* (roadmap 10 ngày và yêu cầu đầu ra).
- *Tổng kết Ngày 5 — 26/10/2026* (kết quả RAW loading và giới hạn đã ghi nhận).
- *Tổng kết dự án đến 28/10/2026* (Ngày 6, dbt staging và Git local).
- Source SQL/Python đã được cung cấp và kiểm tra trong quá trình làm.
- Ảnh `dbt build`, `dbt Docs/Lineage`, Git/GitHub và xác nhận của người thực hiện ở các bước Ngày 7.

*Tài liệu phản ánh trạng thái được cung cấp tại thời điểm cập nhật. Đối với thông số động như row counts, Git commit hash và cloud configuration, luôn ưu tiên dữ liệu/query/log mới nhất trên máy hoặc repository.*
