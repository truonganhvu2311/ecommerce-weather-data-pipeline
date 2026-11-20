# Day 10.5 — Docker/Compose reproducibility templates

These templates describe the **existing local Day 8 Airflow + dbt setup** in a
portable, credential-free form. They are examples, **not yet a tested one-command
deployment on a fresh machine**.

## What was verified

- Three supplied Dockerfiles and three supplied Compose YAML files were inspected.
- Airflow and Google provider versions are pinned to `3.3.2` and `22.5.0`.
- dbt Core/BigQuery adapter versions are pinned to `1.12.6` and `1.12.1`.
- The existing `docker compose config --services` command succeeded on the
  original user's machine (eight always-active services).
- `DBT_ENGINE_TARGET_PATH` and `DBT_ENGINE_LOG_PATH` are valid for dbt 1.12;
  `DBT_ENGINE_PROFILES_DIR` is set in the examples to resolve profiles reliably.
- `Dockerfile.gcp.dbt` depends on a **locally built** image:
  `local/airflow-gcp:3.3.2`. This image must be built first on a new machine.

## Directory arrangement

The repo may store the contents of this directory at
`deployment/airflow/`. The *running* Compose project currently lives in a
separate local directory outside the Git repository.

The existing machine-specific `docker-compose.yaml`,
`docker-compose.override.yaml`, `compose.dbt.yaml`, `.env` and credential files
must **not** be overwritten by these examples.

## Safe review / installation procedure (Windows PowerShell)

1. Keep your current Day 8 Compose folder unchanged. Commit only the sanitized
   example YAMLs, Dockerfiles, this README and the runbook into the Git repo.
2. To test the examples later, use a **separate trial directory** containing
   your already-working Airflow 3.3.2 base `docker-compose.yaml`, plus copies
   of the three Dockerfiles and the two example YAMLs. Copy your existing
   Airflow `.env` to that directory **locally only** and add the four path/project
   variables shown in `env.extra.example`.
3. Run validation before any containers are started:

   ```powershell
   docker compose -f docker-compose.yaml -f docker-compose.override.example.yaml config --quiet
   docker compose -f docker-compose.yaml -f docker-compose.override.example.yaml config --services
   docker compose -f compose.dbt.example.yaml --profile manual config --quiet
   ```

4. If you want to build images **after** the configuration audit, the two-stage
   Airflow build order is:

   ```powershell
   docker build -f Dockerfile.gcp -t local/airflow-gcp:3.3.2 .
   docker compose -f docker-compose.yaml -f docker-compose.override.example.yaml build airflow-worker
   docker compose -f compose.dbt.example.yaml --profile manual build dbt
   ```

   Image builds download packages, may take time, and should be done only
   after the static checks pass. Do not run `docker compose down -v` or reset
   existing state to validate this documentation.

## Operational limitations

- Only `airflow-worker` receives the Google/dbt custom image via the override.
  Other Airflow services use their base image. If new DAGs import Google or dbt
  libraries at DAG-parse time, align their images and retest.
- Within the worker, dbt is isolated at `/opt/dbt-venv/bin/dbt` and is **not**
  installed into Airflow's Python environment. DAG subprocesses should call
  its absolute path.
- The local ADC JSON, private `profiles.yml`, passwords, `.env`, logs and runtime
  state are never public Git artifacts.
- The existing base `docker-compose.yaml` contains local credentials: it must
  be redacted and audited **separately** before it can be made public. This
  package intentionally does **not** include that file.
- The historical weather API is ingested separately, not by each transaction DAG run.
- BigQuery `de_etl.watermarks` is the authoritative committed checkpoint; the
  mounted local `committed_watermarks_verified.json` remains a compatibility
  or evidence artifact and must not be described as the source of truth.
