
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


# Xac dinh thu muc goc cua project
PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_FILE = PROJECT_ROOT / ".env"
STATE_FILE = PROJECT_ROOT / "state" / "watermark.json"
RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "transactions"

# Danh sach bang va khoa chinh
# Ten bang duoc khai bao co dinh trong code
TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

INITIAL_WATERMARK = "1970-01-01T00:00:00+00:00"


# Doc watermark cua lan chay truoc
def load_watermarks():
    if not STATE_FILE.exists():
        return {
            table: INITIAL_WATERMARK
            for table in TABLES
        }

    with STATE_FILE.open("r", encoding="utf-8") as file:
        saved = json.load(file)

    return {
        table: saved.get(table, INITIAL_WATERMARK)
        for table in TABLES
    }


# Luu watermark sau khi trich xuat thanh cong
def save_watermarks(watermarks):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(
        json.dumps(watermarks, indent=2),
        encoding="utf-8"
    )
    temp_file.replace(STATE_FILE)


# Trich xuat cac ban ghi moi hoac da thay doi
def main():
    load_dotenv(ENV_FILE)

    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    database = os.getenv("PGDATABASE", "ecommerce_source")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD")

    if not password:
        raise ValueError(
            "Thieu PGPASSWORD. Hay kiem tra file .env."
        )

    # Doc moc watermark da luu
    old_watermarks = load_watermarks()

    # Tao ID rieng cho lan chay nay
    run_time = datetime.now(timezone.utc)
    run_id = run_time.strftime("%Y%m%dT%H%M%S%fZ")
    run_date = run_time.strftime("%Y-%m-%d")

    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
    )

    new_watermarks = {}

    try:
        with connection.cursor() as cursor:
            # Chot moc thoi gian cho lan trich xuat hien tai
            cursor.execute("SELECT CURRENT_TIMESTAMP")
            current_watermark = cursor.fetchone()[0]

            for table, primary_key in TABLES.items():
                last_watermark = datetime.fromisoformat(
                    old_watermarks[table]
                )

                # Chi lay cac ban ghi trong khoang watermark
                query = f"""
                    SELECT *
                    FROM {table}
                    WHERE updated_at > %s
                      AND updated_at <= %s
                    ORDER BY updated_at, {primary_key}
                """

                cursor.execute(
                    query,
                    (last_watermark, current_watermark)
                )

                columns = [
                    description[0]
                    for description in cursor.description
                ]
                rows = cursor.fetchall()

                if rows:
                    output_dir = (
                        RAW_ROOT
                        / table
                        / f"dt={run_date}"
                    )
                    output_dir.mkdir(
                        parents=True,
                        exist_ok=True
                    )

                    output_file = (
                        output_dir
                        / f"{table}_{run_id}.jsonl"
                    )

                    with output_file.open(
                        "w",
                        encoding="utf-8"
                    ) as file:
                        for row in rows:
                            record = dict(zip(columns, row))

                            # Them metadata de truy vet batch
                            record["_source_table"] = table
                            record["_extracted_at"] = (
                                run_time.isoformat()
                            )
                            record["_watermark_from"] = (
                                last_watermark.isoformat()
                            )
                            record["_watermark_to"] = (
                                current_watermark.isoformat()
                            )

                            file.write(
                                json.dumps(
                                    record,
                                    ensure_ascii=False,
                                    default=str
                                ) + "\n"
                            )

                    print(
                        f"[{table}] {len(rows)} row(s) -> "
                        f"{output_file.relative_to(PROJECT_ROOT)}"
                    )
                else:
                    print(f"[{table}] 0 row(s) thay doi")

                # Chi chuan bi watermark moi.
                # Chua luu file state tai day.
                new_watermarks[table] = (
                    current_watermark.isoformat()
                )

        # Hoan tat truy van truoc khi cap nhat watermark
        connection.commit()

        # Chi luu watermark khi cac bang da xu ly thanh cong
        save_watermarks(new_watermarks)

        print("\nTrich xuat thanh cong.")
        print(f"Watermark moi: {current_watermark.isoformat()}")
        print(f"File state: {STATE_FILE}")

    except Exception:
        connection.rollback()
        print(
            "\nCo loi xay ra. Watermark chua duoc cap nhat."
        )
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()
