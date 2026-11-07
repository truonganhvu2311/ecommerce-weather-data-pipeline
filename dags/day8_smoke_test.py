
# pyrefly: ignore[missing-import]
import pendulum

# pyrefly: ignore[missing-import]
from airflow.sdk import dag, task


@dag(
    dag_id="day8_smoke_test",
    description="Day 8 - First Airflow DAG smoke test",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "smoke-test"],
)
def day8_smoke_test():

    @task
    def prepare():
        print("STEP 1 PASS: Prepare data")
        return {
            "batch_id": "demo_only",
            "status": "prepared"
        }

    @task
    def validate(data):
        assert data["status"] == "prepared"

        print("STEP 2 PASS: Validate data")
        return {
            "batch_id": data["batch_id"],
            "status": "validated"
        }

    @task
    def finish(data):
        assert data["status"] == "validated"

        print("STEP 3 PASS: Finish pipeline")
        print("DAY 8 AIRFLOW SMOKE TEST SUCCESS")

    # Task dependencies
    prepared = prepare()
    validated = validate(prepared)
    finish(validated)


day8_smoke_test()
