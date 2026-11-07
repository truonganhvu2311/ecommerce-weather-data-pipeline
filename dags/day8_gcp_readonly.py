
# pyrefly: ignore[missing-import]
import pendulum

# pyrefly: ignore[missing-import]
from airflow.sdk import dag, task


PROJECT_ID = "weather-data-project-512207"
BUCKET = "weather-data-project-512207-raw"
LOCATION = "asia-southeast1"


@dag(
    dag_id="day8_gcp_readonly",
    description="Read-only GCS and BigQuery connection test",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "gcp", "read-only"],
)
def day8_gcp_readonly():

    @task
    def verify_gcs():
        # pyrefly: ignore[missing-import]
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook(
            gcp_conn_id="google_cloud_default"
        )

        exists = hook.exists(
            bucket_name=BUCKET,
            object_name="manifests/gcs_migration_manifest.csv",
        )

        if not exists:
            raise RuntimeError(
                "GCS migration manifest not found"
            )

        print("GCS READ-ONLY CHECK PASSED")
        return True

    @task
    def verify_bigquery():
        # pyrefly: ignore[missing-import]
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            location=LOCATION,
        )

        client = hook.get_client(
            project_id=PROJECT_ID,
            location=LOCATION,
        )

        dataset = client.get_dataset(
            f"{PROJECT_ID}.de_raw"
        )

        if dataset.location.lower() != LOCATION:
            raise RuntimeError("Wrong BigQuery location")

        client.get_table(
            f"{PROJECT_ID}.de_etl.loaded_files"
        )

        print("BIGQUERY READ-ONLY CHECK PASSED")
        return True

    @task
    def finish(gcs_ok, bq_ok):
        if not (gcs_ok and bq_ok):
            raise RuntimeError("GCP verification failed")

        print("DAY 8.7.6 GCP INTEGRATION PASSED")
        print("No GCS or BigQuery data was modified")

    finish(verify_gcs(), verify_bigquery())


day8_gcp_readonly()
