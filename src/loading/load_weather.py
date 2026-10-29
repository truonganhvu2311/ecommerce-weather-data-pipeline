import json
from datetime import date, datetime
from decimal import Decimal
from getpass import getpass
from pathlib import Path

# pyrefly: ignore[missing-import]
import psycopg


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "weather"
    / "weather_daily.json"
)

UPSERT_SQL = """
INSERT INTO public.weather_daily (
    city,
    weather_date,
    temperature_mean_c,
    rain_sum_mm,
    source,
    extracted_at_utc,
    source_file
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, weather_date)
DO UPDATE SET
    temperature_mean_c = EXCLUDED.temperature_mean_c,
    rain_sum_mm = EXCLUDED.rain_sum_mm,
    source = EXCLUDED.source,
    extracted_at_utc = EXCLUDED.extracted_at_utc,
    source_file = EXCLUDED.source_file,
    loaded_at = CURRENT_TIMESTAMP;
"""


def read_records():
    with INPUT_PATH.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list) or len(records) != 28:
        raise ValueError("Lần thực hành này cần danh sách đúng 28 bản ghi.")

    rows = []
    seen_keys = set()

    for record in records:
        city = record["city"]
        weather_day = date.fromisoformat(record["weather_date"])
        key = (city, weather_day)

        if key in seen_keys:
            raise ValueError(f"Trùng thành phố và ngày: {key}")

        seen_keys.add(key)

        temperature = Decimal(str(record["temperature_mean_c"]))
        rain = Decimal(str(record["rain_sum_mm"]))

        if not temperature.is_finite() or not rain.is_finite():
            raise ValueError(f"{key}: nhiệt độ hoặc lượng mưa không hợp lệ.")

        if rain < 0:
            raise ValueError(f"{key}: lượng mưa không được âm.")

        extracted_at = datetime.fromisoformat(
            record["extracted_at_utc"]
        )

        if extracted_at.utcoffset() is None:
            raise ValueError(f"{key}: thời điểm lấy dữ liệu thiếu múi giờ.")

        rows.append((
            city,
            weather_day,
            temperature,
            rain,
            record["source"],
            extracted_at,
            record["source_file"],
        ))

    return rows


def main():
    # Đọc và kiểm tra file trước khi kết nối database.
    rows = read_records()
    print(f"Đã đọc {len(rows)} bản ghi từ file processed.")

    # Dùng thông tin kết nối giống database chứa orders.
    host = input("Host [localhost]: ").strip() or "localhost"
    port = input("Port [5432]: ").strip() or "5432"
    database = input("Tên database chứa orders: ").strip()
    user = input("User PostgreSQL [postgres]: ").strip() or "postgres"
    password = getpass("Mật khẩu PostgreSQL: ")

    if not database:
        raise ValueError("Bạn chưa nhập tên database.")

    with psycopg.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        connect_timeout=10,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database();")
            print(f"Đang kết nối database: {cursor.fetchone()[0]}")

            cursor.executemany(UPSERT_SQL, rows)

    # Chỉ báo thành công sau khi transaction đã được commit.
    print(f"Đã nạp/cập nhật thành công {len(rows)} bản ghi.")


if __name__ == "__main__":
    main()