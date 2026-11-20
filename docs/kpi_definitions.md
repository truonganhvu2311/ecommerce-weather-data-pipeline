# KPI Definitions, Grains & Reconciliation

**Project:** E-commerce Sales & Weather Analytics Pipeline
**Primary reporting source:** BigQuery `weather-data-project-512207.de_marts`
**Purpose:** Prevent aggregation, date-scope, and weather-null errors in Looker Studio and portfolio explanations.

> **Status:** Field names/types of `fct_orders` and `mart_city_day` were confirmed from BigQuery `INFORMATION_SCHEMA.COLUMNS` screenshots on 20 November 2026. SQL in this file is **read-only**. The final `orders_match`/`amount_match` booleans have **not** been captured yet; do not label them PASS until the query returns `TRUE` for the intended shared scope. The physical `fct_order_items` schema still needs separate confirmation.

## 1. Baseline rules

| Rule | Definition | Impact |
| --- | --- | --- |
| Eligible order | `status IN ('paid', 'shipped', 'delivered') AND is_deleted IS FALSE` | Excludes cancelled, pending, and soft-deleted orders from eligible KPIs |
| Business date | `DATE(order_date, 'Asia/Ho_Chi_Minh')` | Order timestamps are interpreted in Vietnam time for daily grouping |
| Rainy | `rain_sum_mm > 0` | Measured rainfall above zero |
| Dry | `rain_sum_mm = 0` | An observed zero-rainfall city-day |
| Unknown weather | Missing weather association or NULL rainfall | Must not be silently counted as dry |
| Currency / amount units | Defined by the source contract, not verified in the summary | Do not label unconfirmed sample amounts as VND or USD |

The eligibility rule is applied upstream in dbt intermediate/mart logic. Dashboard measures should normally use the already-filtered mart views, without accidentally adding conflicting filters.

## 2. Primary metrics

| KPI | Correct source/grain | Definition | Common mistake |
| --- | --- | --- | --- |
| Eligible Orders | `fct_orders` / one order | `COUNT(*)` at the verified one-row-per-order grain; equivalently `COUNT(DISTINCT order_id)` | Counting item lines as orders |
| Eligible Order Amount | `fct_orders` / one order | **`SUM(total_amount)`** (verified `NUMERIC` field) | Substituting `total_items_amount` or presenting it as recognized revenue |
| Average Order Value (AOV) | `fct_orders` / one order | **`SAFE_DIVIDE(SUM(total_amount), COUNT(*))`** | Averaging group-level AOVs without weights |
| Total Units Sold | `fct_order_items` / item line | `SUM(quantity)` | Using `COUNT(*)` to mean units |
| Total Item-Line Amount | `fct_order_items` / item line | `SUM(line_amount)` | Assuming line amount equals order total |
| Total Order-Item Lines | `fct_order_items` / item line | `COUNT(DISTINCT order_item_id)` | Confusing line count with quantity |
| Distinct Products Sold | `fct_order_items` / item line | `COUNT(DISTINCT product_id)` | Counting all catalog products instead of sold products |
| Orders by City/Date | `mart_city_day` / one city-day | `SUM(order_count)` | Using chart `Record Count` (counts city-days) |
| City-Day Eligible Amount | `mart_city_day` / one city-day | `SUM(total_order_amount)` | Duplicating rows by joining multiple item lines |
| Order-Level Weather Coverage | `mart_city_day` / city-day weighted by orders | `SUM(order_count WHERE has_weather) / SUM(order_count)` | Dividing weather-covered city-days by all city-days instead |

For top-product metrics, sort `SUM(quantity)` and `SUM(line_amount)` **separately**. The leader by units can differ from the leader by item-line value.

## 3. Weather-specific metrics

| KPI | Formula / interpretation |
| --- | --- |
| Rainy Eligible Orders | `SUM(order_count)` on city-days with `rain_sum_mm > 0` |
| Dry Eligible Orders | `SUM(order_count)` on city-days with `rain_sum_mm = 0` |
| Unknown-Weather Eligible Orders | Orders on city-days with missing/unknown weather |
| Weather-Covered Eligible Orders | `SUM(order_count)` for `has_weather = TRUE` |
| Order-Level Coverage Rate | Weather-covered eligible orders / all eligible orders |
| City-Day Row Count | Count of city-day records; **not** order count |

`has_weather = TRUE` means a matching weather record is present; it does **not** guarantee that rainfall and temperature are both non-NULL. Exclude NULL temperature observations from numerical charts rather than plotting misleading zero-degree points.

## 4. Historical demo snapshots (not live totals)

### City & Weather view (1–19 October 2026, all cities)

| Metric | Recorded result |
| --- | ---: |
| Eligible Orders | 7 |
| Hanoi | 3 orders |
| Da Nang | 2 orders |
| Ho Chi Minh City | 2 orders |
| Rainy Orders | 5 |
| Dry Orders | 2 |
| Weather-Covered Orders | 7 |
| Order-Level Weather Coverage | 100% |

The sample is too small for causal weather conclusions or market-wide extrapolation.

### Products & Items view (date scope not independently verified as identical to City & Weather)

| Metric | Recorded result |
| --- | ---: |
| Total Units Sold | 19 |
| Total Item-Line Amount | 12,350,000 **source amount units** |
| Total Order-Item Lines | 14 |
| Distinct Products Sold | 6 |
| Most Units Sold | P002: 6 units |
| Highest Item-Line Amount | P001: 5,000,000 source amount units |

These values were compared with BigQuery query outputs during Day 9. They are **snapshots**, not permanent constants.

**Not yet independently confirmed in the handover:** Exact Sales Overview Eligible Order Amount/AOV and the two final reconciliation booleans described below. Fill them from a current BigQuery run or saved screenshot before describing the entire cross-mart reconciliation as PASS.

## 5. Read-only SQL verification

All queries below target the project and mart dataset recorded in the handover. They **do not modify data**. Verify that the BigQuery query region is compatible with `asia-southeast1`.

### 5.1. Confirm the physical mart schemas first

```sql
SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM `weather-data-project-512207.de_marts.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('fct_orders', 'fct_order_items', 'mart_city_day')
ORDER BY table_name, ordinal_position;
```

Confirmed mappings: `fct_orders.total_amount` (`NUMERIC`), `fct_orders.order_date_vn` (`DATE`), `mart_city_day.total_order_amount` (`NUMERIC`), `mart_city_day.report_date` (`DATE`), and `mart_city_day.city_key` (`STRING`). **`fct_orders.total_items_amount` is a separate `NUMERIC` field, not the order-total KPI.**

### 5.2. Order-level metrics

```sql
SELECT
  COUNT(*) AS eligible_orders,
  SUM(total_amount) AS eligible_order_amount,
  SAFE_DIVIDE(SUM(total_amount), COUNT(*)) AS aov
FROM `weather-data-project-512207.de_marts.fct_orders`;
```

This is an **all-rows** fact check. To compare with a dashboard filtered to 1–19 October 2026, filter on the verified `order_date_vn` field; do not mix all-time and filtered totals.

### 5.3. Item-level metrics

```sql
SELECT
  SUM(quantity) AS total_units,
  SUM(line_amount) AS total_item_line_amount,
  COUNT(DISTINCT order_item_id) AS total_item_lines,
  COUNT(DISTINCT product_id) AS distinct_products
FROM `weather-data-project-512207.de_marts.fct_order_items`;
```

This query uses the item fact's available rows; it does not silently assume the dashboard item page shares the order page's date range.

### 5.4. City-level order totals

```sql
SELECT
  city_key,
  SUM(order_count) AS eligible_orders,
  SUM(total_order_amount) AS eligible_order_amount
FROM `weather-data-project-512207.de_marts.mart_city_day`
GROUP BY city_key
ORDER BY eligible_orders DESC, city_key;
```

### 5.5. Order-level weather coverage

```sql
SELECT
  SUM(order_count) AS all_eligible_orders,
  SUM(IF(has_weather IS TRUE, order_count, 0)) AS weather_covered_orders,
  SAFE_DIVIDE(
    SUM(IF(has_weather IS TRUE, order_count, 0)),
    SUM(order_count)
  ) AS order_weather_coverage_rate
FROM `weather-data-project-512207.de_marts.mart_city_day`;
```

The result is a ratio between 0 and 1; multiply by 100 for a percentage label. Check whether the recorded 100% matches the intended time filter.

### 5.6. Cross-mart reconciliation: `orders_match` and `amount_match`

```sql
WITH order_fact AS (
  SELECT
    COUNT(*) AS eligible_orders,
    SUM(total_amount) AS eligible_amount
  FROM `weather-data-project-512207.de_marts.fct_orders`
  WHERE order_date_vn BETWEEN DATE '2026-10-01' AND DATE '2026-10-19'
),
city_day_mart AS (
  SELECT
    SUM(order_count) AS eligible_orders,
    SUM(total_order_amount) AS eligible_amount
  FROM `weather-data-project-512207.de_marts.mart_city_day`
  WHERE report_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-19'
)
SELECT
  o.eligible_orders AS orders_fact,
  c.eligible_orders AS orders_city_day,
  o.eligible_amount AS amount_fact,
  c.eligible_amount AS amount_city_day,
  o.eligible_orders = c.eligible_orders AS orders_match,
  ABS(COALESCE(o.eligible_amount, 0) - COALESCE(c.eligible_amount, 0)) < 0.01
    AS amount_match
FROM order_fact AS o
CROSS JOIN city_day_mart AS c;
```

**Interpretation:** This specific query matches the **1–19 October 2026** dashboard window in both marts using their verified date columns. Both results should be `TRUE` **only if** the marts represent the same eligible-order population and compatible order-amount semantics. If either is `FALSE`, do not change source data or checkpoints to force a match. First inspect date filters, missing city-day rows, eligibility logic, deduplication, and the grain of each model. For an all-time check, remove **both** date filters together (not just one).

`amount_match` above applies a 0.01 tolerance as an example for decimal comparisons; adjust only if the source precision and business rules justify a different tolerance.

## 6. Looker Studio verification checklist

- [ ] Reset temporary cross-filters before comparing SQL and the dashboard.
- [ ] Confirm all relevant views use the intended date interval and time zone.
- [ ] For `mart_city_day`, use `SUM(order_count)`, never default `Record Count` for orders.
- [ ] For AOV, divide total eligible amount by total eligible orders; do not average city AOV values unweighted.
- [ ] Keep rainy, dry, and unknown weather categories separate.
- [ ] Confirm the Products page's date scope independently from the Sales/Weather pages.
- [ ] Capture the two actual `orders_match` and `amount_match` results before marking reconciliation PASS.
- [ ] Avoid exposing private dataset connections or owner credentials when sharing a report.

Further background is in [Data Dictionary](data_dictionary.md), [Day 9 Business Insights](day9_business_insights.md), and the project's root README.

### Final Reconciliation – 20 November 2026

Analysis period: October 1–19, 2026

- Orders Fact: 7
- Orders City-Day Mart: 7
- Amount Fact: 12,350,000
- Amount City-Day Mart: 12,350,000
- orders_match: TRUE
- amount_match: TRUE

Result: PASS