# Day 9 – Business Insights

## 1. Analysis Overview

**Project:** E-commerce Sales & Weather Analytics Pipeline  
**Purpose:** Evaluate eligible e-commerce orders, product-level purchasing patterns, and their descriptive association with historical weather conditions.  
**Business reporting period (sales and weather):** October 1–19, 2026  
**Data snapshot:** Dashboard and BigQuery results shared on November 17, 2026; metrics may change after subsequent incremental loads.

### Analytical data sources

| BigQuery model | Grain | Main analytical purpose |
|---|---|---|
| `de_marts.fct_orders` | One row per eligible order | Order count, eligible order amount, average order value, and sales by city/date |
| `de_marts.fct_order_items` | One row per eligible order item | Product quantities, item-line amounts, and product rankings |
| `de_marts.mart_city_day` | One row per city and business date | City-level order metrics, weather conditions, and order-level weather coverage |

Eligible orders follow the project's rule: `status IN ('paid', 'shipped', 'delivered') AND is_deleted IS FALSE`. The business date for orders is derived in the `Asia/Ho_Chi_Minh` time zone. The phrase **eligible order amount** is used instead of accounting revenue, because the latter has not been independently established.

**Scope note:** The city and weather observations below refer to the October 1–19 reporting window used in Looker Studio. The product figures are from the observed `fct_order_items` snapshot with **All Products** selected. The product mart's equivalent date filter was not verified in the shared screenshots, so its figures should not be claimed as a date-matched subset until confirmed.

### Observed KPI snapshot

| Indicator | Observed value | Interpretation |
|---|---:|---|
| Eligible orders (city/weather dashboard) | 7 | Orders meeting the project's eligibility criteria |
| Orders associated with weather records | 7 | Orders on city-days where `has_weather = TRUE` |
| Order-level weather coverage | 100% | `7 / 7`; not necessarily complete coverage for every weather measurement |
| Distinct products in the item mart | 6 | Product IDs represented in eligible order items |
| Order-item lines | 14 | Distinct item-level records |
| Units sold | 19 | Sum of `quantity` |
| Total item-line amount | 12,350,000 | Sum of `line_amount`, expressed in the dataset's source monetary units |

## 2. Key Business Insights

### Insight 1 – Eligible Order Distribution by City

**Observation.** Hanoi recorded the largest number of eligible orders in the observed city-and-weather reporting window: **3 out of 7 orders (42.9%)**. Da Nang and Ho Chi Minh City each recorded **2 orders (28.6%)**.

**Quantitative evidence.**

| City | Eligible orders | Share of observed orders |
|---|---:|---:|
| Hanoi | 3 | 42.9% |
| Da Nang | 2 | 28.6% |
| Ho Chi Minh City | 2 | 28.6% |
| **Total** | **7** | **100.0%** |

**Business interpretation.** In this small demonstration sample, Hanoi contributes the highest observed share of eligible order volume. This makes Hanoi a natural starting point for exploring city-level order patterns in the dashboard, including changes over time and differences in weather conditions.

**Limitations.** The difference between Hanoi and either other city is only one order. The sample does not justify statements about long-term market demand, customer purchasing power, or regional performance. No city-level eligible order amount comparison is asserted here because the corresponding values have not been provided and reconciled.

**Evidence source.** `mart_city_day`: `SUM(order_count)` grouped by `city_key`, with the reporting period and city filters held consistent.

### Insight 2 – Eligible Orders Under Rainy and Dry Conditions

**Observation.** The weather dashboard grouped **5 of 7 eligible orders (71.4%)** under **rainy** conditions and **2 orders (28.6%)** under **dry** conditions in the observed reporting window.

**Quantitative evidence.**

| Weather condition | Eligible orders | Share of observed orders |
|---|---:|---:|
| Rainy | 5 | 71.4% |
| Dry | 2 | 28.6% |
| **Total** | **7** | **100.0%** |

**Business interpretation.** More of the observed eligible orders occurred on city-days classified as rainy than on city-days classified as dry. This is a descriptive association that can motivate further investigation, such as comparing **orders per observed city-day** and examining city-specific trends rather than relying on raw totals alone.

**Limitations.** These counts do **not** establish that rain increases purchases. The number of observed rainy and dry city-days may differ; city characteristics, promotional activity, customer behavior, and seasonality are not controlled. With only seven observed orders, the sample is too small for a reliable causal or generalizable conclusion. Missing weather observations must remain a separate `unknown` category where applicable, not be interpreted as dry conditions.

**Evidence source.** `mart_city_day`: `SUM(order_count)` by `weather_condition`. The follow-up validation should also report `COUNT(*) AS city_days` and `SUM(order_count) / COUNT(*) AS orders_per_city_day` for each condition.

### Insight 3 – Concentration of Item-Line Value in Product P001

**Observation.** **P001** contributed **5,000,000** out of the observed **12,350,000** total item-line amount, or approximately **40.5%**. It was the leading product by item-line value, even though the associated quantity was only **2 units**.

**Quantitative evidence.**

| Product | Item-line amount | Share of total item-line amount | Units sold |
|---|---:|---:|---:|
| P001 | 5,000,000 | 40.5% | 2 |
| All other products combined | 7,350,000 | 59.5% | 17 |
| **Total** | **12,350,000** | **100.0%** | **19** |

**Business interpretation.** The observed item-line value is relatively concentrated in P001. This illustrates why product performance should be evaluated using both **monetary contribution** and **units sold**: a product can lead in value without leading in volume. For a richer business analysis, product names, categories, margins, and historical comparisons would be needed.

**Limitations.** `SUM(line_amount)` is not necessarily equivalent to recognized revenue, profit, or the order-level `SUM(total_amount)`. Product identity is currently shown using product IDs because the validated item fact does not establish product names or categories. No claim is made about P001's profitability or sustained commercial importance.

**Evidence source.** `fct_order_items`: `SUM(line_amount)` and `SUM(quantity)` grouped by `product_id`; reconciled against the Product Performance Details table in Looker Studio.

### Insight 4 – Product P002 Leads in Sales Volume, Not Item-Line Value

**Observation.** **P002** recorded the highest quantity sold: **6 of 19 units (31.6%)**. Its item-line amount was **2,100,000**, approximately **17.0%** of the total item-line amount. In contrast, P001 had the highest item-line value despite selling only 2 units.

**Quantitative evidence.**

| Indicator for P002 | Observed value |
|---|---:|
| Units sold | 6 |
| Share of all units | 31.6% |
| Item-line amount | 2,100,000 |
| Share of total item-line amount | 17.0% |
| Distinct order-item lines | 3 |
| Distinct orders containing P002 | 3 |

**Business interpretation.** The volume and value rankings identify different product leaders. P002 may be useful as a high-volume product example, while P001 illustrates higher value concentration. A production dashboard should retain both rankings rather than using one metric as a substitute for the other.

**Limitations.** The item snapshot contains only 19 units across six product IDs. The observed ranking may be highly sensitive to new orders or adjustments. No conclusions are drawn about unit margins, stock demand, pricing strategy, or long-term product popularity without additional data. The 19-unit product snapshot should not be presented as belonging to the October 1–19 window until that date scope is verified.

**Evidence source.** `fct_order_items`: `SUM(quantity)`, `SUM(line_amount)`, `COUNT(DISTINCT order_item_id)`, and `COUNT(DISTINCT order_id)` grouped by `product_id`.

### Insight 5 – Complete Observed Order-to-Weather Matching

**Observation.** The city-and-weather dashboard showed **7 weather-covered orders out of 7 eligible orders**, giving an observed **order-level weather coverage rate of 100%** for the selected date range.

**Quantitative evidence.**

| Coverage measure | Observed value |
|---|---:|
| Total eligible orders | 7 |
| Eligible orders on city-days with `has_weather = TRUE` | 7 |
| Order-level weather coverage | 100% |

**Business interpretation.** All eligible orders in this limited snapshot could be associated with a city-date weather record, supporting the technical feasibility of combining transaction data with historical weather data at the reporting grain. The coverage metric is especially useful as a data-quality indicator alongside weather-based charts.

**Limitations.** This does **not** establish complete weather-field availability, historical API refresh reliability, or future coverage. An existing weather record can still have missing rainfall or temperature fields. The pipeline's end-to-end Airflow DAG is manually triggered in the documented demo and should not be described as continuously scheduled or production-grade on this evidence alone. Coverage must be recalculated after new batches are loaded.

**Evidence source.** `mart_city_day`: `SUM(order_count)` compared with `SUM(CASE WHEN has_weather THEN order_count ELSE 0 END)`, using identical filters.

## 3. Analysis Limitations and Interpretation Guidelines

1. **Small and synthetic sample:** The observed data covers just seven eligible orders in the city/weather analysis and 19 units in the item-level snapshot. Percentage differences can therefore appear large even when absolute differences are small.
2. **Association, not causation:** Rainfall and temperature are joined to sales by city and business date. This join supports descriptive comparison, not causal inference about weather effects on buying behavior.
3. **Eligibility is not accounting revenue:** `fct_orders` excludes orders outside the project's eligibility rule. Eligible order amount, total item-line amount, cash receipts, profit, and recognized revenue are different measures.
4. **Different analytical grains:** `fct_orders` is at order grain; `fct_order_items` is at item grain; `mart_city_day` is at city-date grain. Joining or aggregating these models incorrectly can duplicate monetary values.
5. **Missing weather is not zero rainfall:** Missing values and unmatched records should be represented as `NULL` or `unknown`, never silently converted into dry weather.
6. **Weather coverage has a specific denominator:** The reported 100% is **order-level coverage** for the selected snapshot, not necessarily the share of all city-days covered or the share of complete temperature/rainfall readings.
7. **Time-window alignment must be confirmed:** The sales/weather report is filtered to October 1–19, 2026. The separate item-level snapshot was not shown with a validated matching date filter, so cross-page comparisons should preserve this distinction.
8. **Operational scope:** The Airflow pipeline and recovery behavior have been demonstrated in a controlled, manual-trigger setting. The evidence does not establish always-on scheduling, exactly-once processing across all failures, or production monitoring.

## 4. SQL Validation Evidence

### Validation status

| Check | Current evidence-based status | Required evidence for final sign-off |
|---|---|---|
| Product KPIs and product ranking | **Observed matching** between BigQuery product aggregation and Looker Studio screenshots | Retain the final corrected Product Performance screenshot and SQL query result |
| Da Nang city-day weather rows | **Observed matching** in the provided five-row SQL result and weather-condition grouping | Retain query screenshots |
| Seven eligible orders by city and weather condition | **Observed matching** across dashboard summaries | Save all-city SQL aggregations using the same October 1–19 date filter |
| Order-level weather coverage | **Observed as 100%** in the dashboard | Save SQL returning `total_orders`, `orders_with_weather`, and `weather_coverage_pct` |
| Cross-mart order-count and eligible-amount reconciliation | **Not yet evidenced as fully passed in the materials reviewed for this document** | Save a BigQuery result with `orders_match = TRUE` and `amount_match = TRUE` |

### Recommended screenshot references

Keep or update the following filenames to match the actual screenshots saved in the repository's `docs/screenshots/` directory:

- `day9_11_sales_overview.png` — sales KPI and time-series dashboard.
- `day9_20_city_weather_overview.png` — city/weather charts and filters.
- `day9_27_products_overview.png` — item-level KPI cards and product rankings.
- `day9_28_sales_city_reconciliation.png` — cross-mart order count and amount comparison.
- `day9_29_weather_coverage_validation.png` — weather-coverage SQL calculation.
- `day9_30_weather_condition_analysis.png` — rainy/dry city-days, order counts, and order rates.

**Evidence note:** These paths are suggested names from the project workflow, not a claim that every screenshot has been verified on disk. Update the filenames or insert Markdown image links only after confirming the actual files exist.

**Portfolio summary:** This project demonstrates how an Airflow-orchestrated ELT workflow can prepare BigQuery/dbt analytical marts for a multi-page Looker Studio report, and how cross-model validation and explicitly qualified insights make the resulting dashboards more trustworthy and explainable.
