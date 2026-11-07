
# pyrefly: ignore[missing-import]
import pendulum

# pyrefly: ignore[missing-import]
from airflow.sdk import dag, task


@dag(
    dag_id="day8_postgres_readonly",
    description="Test PostgreSQL SOURCE connection safely",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["day8", "postgres", "read-only"],
)
def postgres_readonly_dag():

    @task
    def check_postgres():
        # pyrefly: ignore[missing-import]
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce_source"
        )

        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                # Make this transaction read-only.
                cursor.execute("SET TRANSACTION READ ONLY")

                # Verify the actual source database.
                cursor.execute("""
                    SELECT current_database(), inet_server_port()
                """)

                db_name, server_port = cursor.fetchone()

                if db_name != "ecommerce_source":
                    raise ValueError(
                        f"Wrong database: {db_name}"
                    )

                print(f"Connected database: {db_name}")
                print(f"PostgreSQL server port: {server_port}")

                # Read only aggregate counts, no personal data.
                cursor.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM customers),
                        (SELECT COUNT(*) FROM products),
                        (SELECT COUNT(*) FROM orders),
                        (SELECT COUNT(*) FROM order_items)
                """)

                result = cursor.fetchone()

                counts = {
                    "customers": result[0],
                    "products": result[1],
                    "orders": result[2],
                    "order_items": result[3],
                }

                print(f"PostgreSQL counts: {counts}")
                return counts

        finally:
            conn.rollback()
            conn.close()

    @task
    def validate_counts(counts):
        for table_name, count in counts.items():
            assert count >= 0, (
                f"Invalid count for {table_name}"
            )

        print("POSTGRESQL READ-ONLY TEST PASSED")
        print(f"Validated tables: {len(counts)}")

    validate_counts(check_postgres())


postgres_readonly_dag()
