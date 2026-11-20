# Data Dictionary & Logical Lineage

**Project:** E-commerce Sales & Weather Analytics Pipeline
**Document status:** Updated from the handbook, Day 1–9 handover, and BigQuery `INFORMATION_SCHEMA.COLUMNS` screenshots supplied on 20 November 2026.
**Scope:** Transaction source, historical weather source, BigQuery layers, and analytical marts.

> **Verification boundary:** The field names and types of **`fct_orders` and `mart_city_day`** below were verified against the supplied BigQuery `INFORMATION_SCHEMA` screenshots. Nullability, `fct_order_items` fields, source constraints, and exact dbt SQL dependencies still require direct inspection. Do not describe this as a complete physical schema audit of all models.

## 1. Data sources and table grains

| Source object | Grain (one row represents) | Logical key | Description |
| --- | --- | --- | --- |
| PostgreSQL `customers` | One source customer | `customer_id` | Customer reference and home/city attribute |
| PostgreSQL `products` | One source product | `product_id` | Product catalog, category and list price |
| PostgreSQL `orders` | One order | `order_id` | Order status, business timestamp, location, order-level amount and soft deletion |
| PostgreSQL `order_items` | One product line within an order | `order_item_id` | Quantity and unit price for the line |
| Open-Meteo historical daily response | One requested location with arrays of daily observations | Location + date after normalization | Daily temperature/rainfall context for analysis |

Business IDs such as `C990`, `P001` and order IDs are **strings**, not arbitrary integers. In BigQuery, the relevant business-key columns were standardized to `STRING` during cloud migration.

## 2. Source-field dictionary

The fields below are documented from the project's source-schema specification and handover. Verify physical SQL types and constraints in `sql/source_schema.sql`.

### `customers`

| Field | Logical meaning | Notes |
| --- | --- | --- |
| `customer_id` | Customer business key | Treat as `STRING` in warehouse models |
| `customer_name` | Customer display name | Not a metric or an analytical key |
| `city` | Customer city | May differ from the city recorded on an order |
| `created_at` | Source creation timestamp | Audit / insertion context |
| `updated_at` | Latest source update timestamp | Used for timestamp-based incremental selection |

### `products`

| Field | Logical meaning | Notes |
| --- | --- | --- |
| `product_id` | Product business key | Treat as `STRING` in warehouse models |
| `product_name` | Product display name | Dashboard product label |
| `category` | Product category | Dimension, when present in analytical output |
| `price` | Source product price | Product-list price; not necessarily the historical sale price |
| `created_at` | Source creation timestamp | Audit context |
| `updated_at` | Latest source update timestamp | Incremental extraction input |

### `orders`

| Field | Logical meaning | Notes |
| --- | --- | --- |
| `order_id` | Order business key | One order, potentially multiple item lines |
| `customer_id` | Customer foreign key | Relationship to `customers` |
| `order_date` | Source order timestamp | Convert to Vietnam business date for reporting |
| `status` | Order lifecycle state | Eligible: `paid`, `shipped`, `delivered` |
| `city` | City associated with the order | Prefer the order-level city for city-based order KPIs |
| `total_amount` | Total amount recorded on the order | Do not assume it equals the sum of item-line amounts |
| `is_deleted` | Soft-delete indicator | `TRUE` means excluded from eligible/active-order KPIs |
| `created_at` | Source creation timestamp | Audit context |
| `updated_at` | Latest source update timestamp | Incremental extraction input |

### `order_items`

| Field | Logical meaning | Notes |
| --- | --- | --- |
| `order_item_id` | Item-line business key | One line within one order |
| `order_id` | Parent order key | Join to `orders` |
| `product_id` | Product key | Join to `products` |
| `quantity` | Number of units on the line | Aggregate with `SUM` after eligibility filters |
| `unit_price` | Sale unit price recorded on the line | May differ from current catalog price |
| `created_at` | Source creation timestamp | Audit context |
| `updated_at` | Latest source update timestamp | Incremental extraction input |

### Weather payload

| Field/path | Meaning | Null / unit handling |
| --- | --- | --- |
| Location / city metadata | Geographic join context | Normalize to the project's city names before joining |
| `daily.time[]` | Daily observation date | Convert to the intended calendar date consistently |
| `daily.rain_sum[]` | Daily precipitation | Modeled as `rain_sum_mm` in the reporting logic; NULL is unknown, **not 0 mm** |
| `daily.temperature_2m_mean[]` | Mean daily temperature | NULL means missing; **not 0°C** |

Open-Meteo RAW JSON can hold multiple daily observations per response; transformation/unnesting is required to reach one **city + date** weather row.

## 3. Ingestion metadata and checkpoints

| Component | Meaning | Contract |
| --- | --- | --- |
| JSONL RAW batch | Extracted transactional records | Preserves source rows and per-extract metadata |
| Manifest | Batch/file identity, row counts and SHA-256 checksum | Supports verification and replay analysis |
| `de_etl.loaded_files` | Loaded-file registry | Protects against duplicate/changed-file replays within the demonstrated design |
| `de_etl.pipeline_batches` | Batch execution lifecycle | Tracks batch processing/finalization |
| `de_etl.watermarks` | Four committed source-table checkpoints | **Authoritative** state for current Airflow incremental processing |

The production-oriented extractor for the demonstrated Airflow path is `src/ingestion/extract_incremental_airflow.py`. The older `src/ingestion/extract_incremental.py` is legacy and should not be used to advance the current checkpoint state.

## 4. BigQuery model inventory

### RAW and metadata

| Dataset | Tables | Role |
| --- | --- | --- |
| `de_raw` | `customers`, `products`, `orders`, `order_items`, `weather_api_responses` | Loaded source/response history |
| `de_etl` | `loaded_files`, `pipeline_batches`, `watermarks` | Processing metadata and committed state |

### dbt layers

| Layer | Model | Conceptual grain / purpose |
| --- | --- | --- |
| Staging | `stg_customers` | Normalized/latest customer state |
| Staging | `stg_products` | Normalized/latest product state |
| Staging | `stg_orders` | Normalized/latest order state, including status and soft-delete information |
| Staging | `stg_order_items` | Normalized/latest item-line state |
| Staging | `stg_weather_daily` | Weather observations normalized to city + date |
| Intermediate | `int_orders_eligible` | Apply the eligible-order business rule |
| Intermediate | `int_order_items_by_order` | Prepare reusable item-level/order-level enrichment |
| Intermediate | `int_orders_weather` | Combine eligible-order context with city-day weather |
| Mart | `fct_orders` | One eligible order per row |
| Mart | `fct_order_items` | One eligible order-item line per row |
| Mart | `mart_city_day` | One city and one business day per row |

These are the 11 dbt models recorded as built and tested in the handover (5 + 3 + 3). The **exact physical columns and individual `ref()` edges** should be confirmed against the BigQuery dbt SQL and `dbt docs` lineage graph.

## 5. Mart-level analytical dictionary

| Mart | Logical field / measure | Meaning and aggregation |
| --- | --- | --- |
| `fct_orders` | `order_id` | Unique eligible-order key; `COUNT(*)` is valid at the asserted grain |
| `fct_orders` | `total_amount` | **Verified `NUMERIC` order-level amount**; sum only once per eligible order |
| `fct_orders` | `order_date_vn`, `city` | **Verified** Vietnam business date (`DATE`) and order city (`STRING`) |
| `fct_orders` | `total_items_amount` | **Different field:** derived/aggregated item amount (`NUMERIC`); do not substitute it for `total_amount` in order-amount reconciliation |
| `fct_order_items` | `order_item_id` | Unique eligible item-line key |
| `fct_order_items` | `product_id` | Product dimension for volume/value rankings |
| `fct_order_items` | `quantity` | Units sold; use `SUM(quantity)` |
| `fct_order_items` | `line_amount` | Item-line amount; use `SUM(line_amount)` |
| `mart_city_day` | `city_key`, `report_date` | **Verified** city (`STRING`) + reporting day (`DATE`) composite grain |
| `mart_city_day` | `order_count` | Orders associated with the city-day; **use `SUM(order_count)`**, not chart Record Count |
| `mart_city_day` | `total_order_amount` | **Verified `NUMERIC`** aggregate of order amounts for the city-day |
| `mart_city_day` | `rain_sum_mm` | Rainfall in mm; positive=rainy, zero=dry, NULL=unknown |
| `mart_city_day` | `has_weather` | Whether the city-day matched a weather record; does not promise all weather attributes are non-NULL |

### Verified BigQuery mart column inventory (20 November 2026)

The following fields and `DATA_TYPE` values were directly visible in the screenshots of `INFORMATION_SCHEMA.COLUMNS`. Nullability was not included in those screenshots.

**`de_marts.fct_orders` (21 fields)**

| Field | Verified type | Field | Verified type |
| --- | --- | --- | --- |
| `order_id` | `STRING` | `customer_id` | `STRING` |
| `order_date` | `TIMESTAMP` | `status` | `STRING` |
| `city` | `STRING` | `total_amount` | `NUMERIC` |
| `created_at` | `TIMESTAMP` | `updated_at` | `TIMESTAMP` |
| `is_deleted` | `BOOL` | `_extracted_at` | `TIMESTAMP` |
| `loaded_at` | `TIMESTAMP` | `raw_event_id` | `STRING` |
| `source_file` | `STRING` | `source_line_number` | `INT64` |
| `order_datetime_vn` | `DATETIME` | `order_date_vn` | `DATE` |
| `item_count` | `INT64` | `distinct_product_count` | `INT64` |
| `total_quantity` | `INT64` | `total_items_amount` | `NUMERIC` |
| `has_order_items` | `BOOL` | | |

**`de_marts.mart_city_day` (15 fields)**

| Field | Verified type | Description |
| --- | --- | --- |
| `city_key` | `STRING` | Normalized city key |
| `report_date` | `DATE` | City-day reporting date |
| `order_count` | `INT64` | Eligible order count for the city-day |
| `total_order_amount` | `NUMERIC` | Order-level amount aggregated to the city-day |
| `avg_order_value` | `NUMERIC` | City-day average order value; do not average unweighted across days |
| `paid_order_count` | `INT64` | Paid-order count |
| `shipped_order_count` | `INT64` | Shipped-order count |
| `delivered_order_count` | `INT64` | Delivered-order count |
| `temperature_mean_c` | `FLOAT64` | Mean temperature, NULL if unavailable |
| `rain_sum_mm` | `FLOAT64` | Daily precipitation, NULL if unavailable |
| `weather_source` | `STRING` | Weather provenance |
| `weather_source_file` | `STRING` | Weather RAW file provenance |
| `has_orders` | `BOOL` | City-day has eligible orders |
| `has_weather` | `BOOL` | City-day has a matching weather record |
| `weather_condition` | `STRING` | Rainy/dry/unknown-style weather grouping |

See [KPI Definitions](kpi_definitions.md) for the corrected **`fct_orders.total_amount` vs `mart_city_day.total_order_amount`** formula and reconciliation SQL.

## 6. Logical lineage overview

| Source domain | Staging | Intermediate logic | Mart consumers |
| --- | --- | --- | --- |
| Orders + customers | `stg_orders`, `stg_customers` | Order eligibility and customer context | `fct_orders`, `mart_city_day` |
| Order items + products | `stg_order_items`, `stg_products` | Item-to-order enrichment | `fct_order_items` |
| Historical weather | `stg_weather_daily` | City-day weather association | `mart_city_day` |

This is a **conceptual lineage summary**, not a claim that every model directly references every upstream table. For exact parent-child SQL dependencies, inspect `source()`/`ref()` calls and the dbt-generated lineage graph.

## 7. Data semantics and quality controls

1. **Eligible orders:** `status IN ('paid', 'shipped', 'delivered') AND is_deleted IS FALSE`.
2. **Business date:** `DATE(order_date, 'Asia/Ho_Chi_Minh')` for timestamps interpreted as BigQuery `TIMESTAMP`.
3. **Weather:** Rainy if `rain_sum_mm > 0`, dry if `rain_sum_mm = 0`, unknown if NULL/missing.
4. **Grain discipline:** Order, order-item line, and city-day facts are not interchangeable.
5. **Financial label:** Eligible order amount is not audited/accounting-recognized revenue. The currency/units of sample amounts should not be inferred without confirming the source contract.
6. **CDC scope:** This is timestamp-based incremental processing plus soft deletes—not WAL/logical-decoding CDC and not automatic hard-delete capture.
7. **Validation:** 11 models + 76 dbt tests produced **87/87 successful results** in recorded demo executions. This is a historical run result, not continuous production monitoring.
8. **Recovery:** Local sandbox/retry demonstrations do not prove exactly-once guarantees for all cloud failures and concurrent runs.

## 8. Physical schema verification checklist

Before declaring this a *complete physical data dictionary*, confirm and fill in:

- [x] Actual BigQuery field names and `DATA_TYPE` for `fct_orders` and `mart_city_day` from supplied `INFORMATION_SCHEMA.COLUMNS` screenshots.
- [ ] Nullability for those models and full `fct_order_items` physical column inventory.
- [ ] Source PostgreSQL column types, PK/FK/unique constraints and nullability in `sql/source_schema.sql`.
- [ ] Whether `line_amount` is computed from `quantity * unit_price` or another explicit pricing rule in `fct_order_items.sql`.
- [x] Physical date/city field names in `mart_city_day`: `report_date` and `city_key`.
- [x] Confirmed `rain_sum_mm`/`has_weather` are physical output columns of `mart_city_day`.
- [ ] Validate how city normalization is implemented inside the dbt SQL.
- [ ] Current `orders_match` and `amount_match` SQL results, using the same filters and time range.
- [ ] README links and screenshot paths after the new documents are copied into the repository.

This checklist prevents documenting inferred physical details as verified facts. Use [KPI Definitions](kpi_definitions.md) for the corresponding safe read-only reconciliation queries.
