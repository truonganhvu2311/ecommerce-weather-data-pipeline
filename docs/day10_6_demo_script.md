# Project Demo Script & Interview Q&A

**Project:** E-commerce Sales & Weather Analytics Pipeline

**Role:** Personal Data Engineering Project (2026)

**Suggested duration:** 7–8 minutes, followed by questions
**Data window:** October 1–19, 2026 (demo snapshot)

> Portfolio demonstration, not a claim of production readiness. Use existing screenshots and read-only pages; do not re-trigger ingestion, update watermarks, or expose credentials just to present this project.

## A. Five-minute pre-demo preparation

Open these materials before presenting:

1. GitHub root `README.md` and `docs/architecture.png`.
2. One successful Airflow run screenshot (for example `docs/screenshots/day8_auto_pipeline_nonempty_success.png`).
3. dbt validation evidence (for example `docs/screenshots/day8_auto_pipeline_dbt_87_pass.png`).
4. The three Looker Studio dashboard pages: Sales Overview, City & Weather, Products & Items. If sharing is restricted, use saved screenshots instead.
5. Optional reconciliation screenshot showing `orders_match = true` and `amount_match = true`, based on the same eligible-status/date filters.

Confirm the screenshot names and dashboard accessibility in your repository before the interview. Do **not** make a private dashboard public simply to perform a demo.

## B. Seven-to-eight-minute English presentation script

### 0:00–0:45 — Business problem

"Hello, my name is Trung. Today I will walk you through my personal Data Engineering project, *E-commerce Sales & Weather Analytics Pipeline*.

The goal is to transform transactional e-commerce records and historical weather observations into trustworthy analytical datasets. The output helps explore sales performance by city, date, and product, and compare sales with weather conditions. The transaction dataset is a small synthetic demo sample, so the weather findings are descriptive rather than causal."

**Show:** GitHub README and the business questions.

### 0:45–1:45 — Architecture

"The main stack is PostgreSQL, Python, Google Cloud Storage, BigQuery, dbt, Apache Airflow, Docker, and Looker Studio.

PostgreSQL stores the transactional source tables. Python incrementally extracts changed records into JSONL batches. Google Cloud Storage stores the raw files, and BigQuery organizes the data into raw, staging, intermediate, and marts layers. dbt transforms and tests the analytical models. Looker Studio visualizes the business metrics.

Historical weather data comes from the Open-Meteo API through a separate ingestion process. My current Airflow transaction DAG does not fetch new weather observations on each execution."

**Show:** `docs/architecture.png`, emphasizing that the weather ingestion branch is separate.

### 1:45–2:45 — Incremental extraction and data correctness

"For transactional changes, I implemented timestamp-based incremental extraction with committed watermarks and soft-delete handling. This avoids repeatedly extracting unchanged records while retaining a way to represent deletes in the demo.

Each batch uses metadata and file-level checks so repeated processing can be detected. Importantly, this is a learning-oriented incremental pattern, not PostgreSQL WAL-based change data capture. It does not automatically detect every hard delete or late-committed transaction."

**Show:** Extractor/manifest design in documentation; avoid opening private connection files.

### 2:45–4:10 — Orchestration, tests, and recovery

"I use Airflow 3.3.2 in Docker to coordinate the transactional pipeline. The DAG is manually triggered. Its tasks handle extraction, object storage, warehouse loading, dbt transformations, and quality checks.

The critical reliability rule is that a committed watermark advances only after the downstream quality gate passes. BigQuery's `de_etl.watermarks` is the authoritative committed state. I verified both empty and non-empty batch paths, and practiced controlled retry and recovery scenarios.

For dbt, the documented validation run completed 11 models and 76 tests, with 87 of 87 successful executions. The recovery drills provide evidence for this demo setup; they are not proof of exactly-once processing under every possible cloud failure."

**Show:** Airflow success screenshots and the dbt 87/87 evidence.

### 4:10–5:15 — Analytics models and reconciliation

"The main business-facing models include `de_marts.fct_orders`, `de_marts.fct_order_items`, and `de_marts.mart_city_day`. Their grains differ: an order, an order line, and a city-date summary.

I explicitly checked reconciliation between eligible orders and the city-date mart using consistent filters. In the saved October 1–19 snapshot, both sides reported seven eligible orders and a total order amount of 12,350,000 VND. The order-count and amount comparisons both returned true.

This is useful because a seemingly reasonable join between order and item facts can otherwise multiply an order-level amount."

**Show:** `docs/data_dictionary.md`, `docs/kpi_definitions.md`, and the SQL reconciliation evidence.

### 5:15–6:30 — Dashboards and insights

"I built three reporting pages in Looker Studio: Sales Overview, City & Weather, and Products & Items.

They provide city-level sales measures, order trends, product contributions, and weather coverage indicators. The written analysis records five business insights along with supporting evidence and limitations.

Since the transactions are synthetic and the sample size is small, the dashboards demonstrate analytics engineering and validation practices. They do not establish that weather causes sales to increase or decrease."

**Show:** The three dashboards or saved Day 9 screenshots. Keep the date range and city filter consistent.

### 6:30–7:30 — Lessons learned and next improvements

"The most valuable lessons were preserving data grain, validating transformations, making retries safer, and delaying watermark commits until downstream checks pass.

The current system is suitable as a portfolio demonstration. For a production-oriented version, I would evaluate WAL-based CDC, more robust late-arriving data handling, scheduled orchestration, CI checks, cloud monitoring, cost controls, and concurrency testing. I would also validate real recovery scenarios and dashboard access policies.

Thank you. I would be happy to explain any part of the architecture, data quality strategy, or trade-offs."

**Show:** GitHub README limitations and future improvements.

## C. Interview Q&A — English answers

**Q1. Why did you choose a timestamp-based incremental approach instead of Debezium or WAL CDC?**

I chose it to implement and validate the full incremental workflow within a manageable personal project. It is easier to understand and demonstrate, but it has known limitations for late commits and hard deletes. WAL-based CDC would be a next-step improvement.

**Q2. How do you handle DELETE operations?**

The demo uses soft-delete flags so deleted business records can be represented in incremental batches. Hard deletes without tombstones are not fully captured by this approach.

**Q3. What happens if a pipeline task fails after data has been uploaded?**

The pipeline tracks batch metadata, uses replay safeguards, and does not commit the next watermark until downstream checks succeed. I tested retries and controlled failure scenarios, but I would not claim exactly-once behavior across all cloud outages.

**Q4. Why is the watermark committed after dbt tests?**

Because a successfully extracted batch is not necessarily analytically correct. Delaying the checkpoint protects the next incremental run from skipping records before warehouse loading and quality validation have completed.

**Q5. How did you avoid double-counting revenue?**

I documented the grain of each model and reconciled eligible order-level metrics with the city-day mart under matching filters. I avoid summing order totals after joining to multiple order-item rows.

**Q6. Did you prove that weather changes sales?**

No. My dashboard shows descriptive associations using a small synthetic dataset. It does not establish causation, and the weather data can also have missing coverage or unknown conditions.

**Q7. Does Airflow run automatically every day?**

Not currently. The main transaction DAG uses manual triggers. Scheduling and the monitoring required for unattended operation are future improvements.

**Q8. How do you protect Google Cloud credentials?**

The working environment keeps ADC and private profiles outside the public repository. The GitHub deployment examples use placeholders and environment variables instead of embedding credential files.

**Q9. What is your strongest contribution in this project?**

The integration of incremental extraction with downstream quality-gated checkpoint commits, supported by reconciliation, dbt tests, and controlled recovery evidence.

**Q10. What would you improve first if this had real users?**

I would strengthen CDC correctness and observability first, then add scheduled operation, CI checks, alerts, cost controls, access review, and tests for concurrency and real cloud failures.

## D. Evidence and accuracy checklist

- [ ] Screenshots really exist at the cited paths and do not expose credentials.
- [ ] dbt evidence supports the stated 11 models, 76 tests, and 87/87 successful executions **for that run**.
- [ ] Reconciliation evidence uses equivalent date/eligible-order filters and returns matching counts and amounts.
- [ ] The October 1–19 amounts are labeled as a demo **snapshot**, not permanent live values.
- [ ] Airflow is described as manual-trigger, not fully scheduled.
- [ ] Open-Meteo history is described as a separate ingestion path.
- [ ] Timestamp incremental is not described as WAL CDC, and file-level replay safeguards are not described as universal exactly-once.
- [ ] Looker Studio sharing permissions are understood before sending a live report link.
- [ ] Public GitHub files contain no ADC JSON, private profiles, tokens, or passwords.

## E. Optional short CV summary

Built a portfolio-scale incremental ELT pipeline integrating PostgreSQL, Python, GCS, BigQuery, dbt, Apache Airflow, and Looker Studio. Implemented soft-delete-aware timestamp extraction, batch replay safeguards, QA-gated BigQuery watermark commits, analytical marts, reconciliation checks, and a three-page sales/weather dashboard. Demonstrated 87/87 successful dbt model/test executions and controlled Airflow retry/recovery scenarios.
