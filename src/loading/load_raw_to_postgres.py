import hashlib
import json
import os
import subprocess
import tempfile

from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from dotenv import load_dotenv


# ==========================================
# 1. CONFIGURATION
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

BUCKET_URI = (
    "hf://buckets/AnhVu2006/weather-project-raw"
)

WAREHOUSE_DB = os.getenv("PG_WAREHOUSE_DB")

if not WAREHOUSE_DB:
    raise ValueError(
        "Thieu PG_WAREHOUSE_DB trong file .env"
    )


# Các cột dữ liệu gốc của từng bảng
TABLE_COLUMNS = {
    "customers": [
        "customer_id",
        "customer_name",
        "city",
        "created_at",
        "updated_at",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "price",
        "created_at",
        "updated_at",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "city",
        "total_amount",
        "created_at",
        "updated_at",
        "is_deleted",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "created_at",
        "updated_at",
    ],
}

# Metadata đã được extract_incremental.py tạo ra
METADATA_COLUMNS = [
    "_source_table",
    "_extracted_at",
    "_watermark_from",
    "_watermark_to",
]


# ==========================================
# 2. DATABASE CONNECTION
# ==========================================

def connect_warehouse():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=WAREHOUSE_DB,
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
    )


# ==========================================
# 3. CHECKSUM
# ==========================================

def calculate_sha256(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


# ==========================================
# 4. DOWNLOAD FROM HUGGING FACE
# ==========================================

def download_raw_files(download_dir):
    print("\n[1] Downloading raw files from Hugging Face...")

    subprocess.run(
        [
            "hf",
            "buckets",
            "sync",
            BUCKET_URI,
            str(download_dir),
        ],
        check=True,
    )

    print("Download completed.")


# ==========================================
# 5. LOAD TRANSACTION JSONL
# ==========================================

def load_transaction_file(
    cursor,
    file_path,
    source_file,
    table_name,
):
    columns = (
        TABLE_COLUMNS[table_name]
        + METADATA_COLUMNS
        + ["source_file", "source_line_number"]
    )

    placeholders = sql.SQL(", ").join(
        sql.Placeholder() for _ in columns
    )

    query = sql.SQL(
        "INSERT INTO raw.{} ({}) VALUES ({})"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(
            sql.Identifier(column) for column in columns
        ),
        placeholders,
    )

    record_count = 0

    with open(file_path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                raise ValueError(
                    f"Empty line: {source_file}:{line_number}"
                )

            record = json.loads(line)

            if record["_source_table"] != table_name:
                raise ValueError(
                    f"Invalid source table: {source_file}"
                )

            values = [
                record[column]
                for column in (
                    TABLE_COLUMNS[table_name]
                    + METADATA_COLUMNS
                )
            ]

            values.extend([
                source_file,
                line_number,
            ])

            cursor.execute(query, values)

            record_count += 1

    return record_count


# ==========================================
# 6. LOAD WEATHER JSON
# ==========================================

def load_weather_file(
    cursor,
    file_path,
    source_file,
):
    with open(file_path, "r", encoding="utf-8") as file:
        payload = json.load(file)

    if (
        not isinstance(payload, dict)
        or "metadata" not in payload
        or "response" not in payload
    ):
        raise ValueError(
            f"Invalid weather JSON: {source_file}"
        )

    cursor.execute(
        """
        INSERT INTO raw.weather_api_responses
            (source_file, payload)
        VALUES (%s, %s)
        """,
        (
            source_file,
            Json(payload),
        ),
    )

    return 1


# ==========================================
# 7. PROCESS ONE FILE
# ==========================================

def process_file(connection, file_path, download_dir):
    relative_path = file_path.relative_to(download_dir)

    source_file = relative_path.as_posix()
    file_sha256 = calculate_sha256(file_path)

    # Xác định loại file
    if file_path.suffix == ".jsonl":

        parts = relative_path.parts

        if (
            len(parts) < 5
            or parts[0] != "raw"
            or parts[1] != "transactions"
            or parts[2] not in TABLE_COLUMNS
        ):
            raise ValueError(
                f"Unexpected transaction path: {source_file}"
            )

        source_type = parts[2]

    elif file_path.suffix == ".json":
        source_type = "weather"

    else:
        print(f"[SKIP] Unsupported file: {source_file}")
        return "ignored", 0

    # Mỗi file là một database transaction riêng.
    with connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT sha256
                FROM etl.loaded_files
                WHERE source_file = %s
                """,
                (source_file,),
            )

            existing = cursor.fetchone()

            if existing is not None:

                existing_sha256 = existing[0].strip()

                if existing_sha256 == file_sha256:
                    print(f"[SKIP] Already loaded: {source_file}")
                    return "skipped", 0

                raise ValueError(
                    "File changed after loading: "
                    + source_file
                )

            # Nạp dữ liệu vào bảng raw
            if source_type == "weather":

                record_count = load_weather_file(
                    cursor,
                    file_path,
                    source_file,
                )

            else:

                record_count = load_transaction_file(
                    cursor,
                    file_path,
                    source_file,
                    source_type,
                )

            # Chỉ ghi nhận file sau khi insert raw thành công.
            # Hai thao tác sẽ được commit cùng nhau.
            cursor.execute(
                """
                INSERT INTO etl.loaded_files
                    (
                        source_file,
                        source_type,
                        sha256,
                        record_count
                    )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    source_file,
                    source_type,
                    file_sha256,
                    record_count,
                ),
            )

    print(
        f"[LOADED] {source_file}: "
        f"{record_count} record(s)"
    )

    return "loaded", record_count


# ==========================================
# 8. MAIN
# ==========================================

def main():
    print("=" * 50)
    print("RAW DATA LOADER")
    print("=" * 50)

    # Mỗi lần chạy sử dụng thư mục tải tạm mới.
    # Tránh đọc nhầm file còn sót lại từ lần chạy trước.
    with tempfile.TemporaryDirectory(
        prefix="weather_project_raw_"
    ) as temp_dir:

        download_dir = Path(temp_dir)

        download_raw_files(download_dir)

        files = sorted(
            path
            for path in download_dir.rglob("*")
            if path.is_file()
        )

        if not files:
            raise RuntimeError(
                "No files downloaded from Hugging Face"
            )

        print(f"\nFound {len(files)} file(s).")

        connection = connect_warehouse()

        try:
            loaded_files = 0
            skipped_files = 0
            loaded_records = 0

            for file_path in files:

                status, count = process_file(
                    connection,
                    file_path,
                    download_dir,
                )

                if status == "loaded":
                    loaded_files += 1
                    loaded_records += count

                elif status == "skipped":
                    skipped_files += 1

            print("\n========== SUMMARY ==========")
            print(f"Loaded files: {loaded_files}")
            print(f"Skipped files: {skipped_files}")
            print(f"Loaded records: {loaded_records}")
            print("=============================")

        finally:
            connection.close()


if __name__ == "__main__":
    main()