import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests


API_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEZONE = "Asia/Ho_Chi_Minh"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "weather"

# Tọa độ đại diện cho từng thành phố.
CITY_CONFIGS = [
    {
        "city": "Hanoi",
        "slug": "hanoi",
        "latitude": 21.0285,
        "longitude": 105.8542,
        "start_date": "2026-10-01",
        "end_date": "2026-10-19",
    },
    {
        "city": "Da Nang",
        "slug": "da_nang",
        "latitude": 16.0544,
        "longitude": 108.2022,
        "start_date": "2026-10-02",
        "end_date": "2026-10-06",
    },
    {
        "city": "Ho Chi Minh City",
        "slug": "ho_chi_minh_city",
        "latitude": 10.8231,
        "longitude": 106.6297,
        "start_date": "2026-10-02",
        "end_date": "2026-10-05",
    },
]


def get_expected_dates(start_date, end_date):
    """Tạo danh sách các ngày cần nhận, gồm cả ngày cuối."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if end < start:
        raise ValueError("Ngày kết thúc phải từ ngày bắt đầu trở đi.")

    number_of_days = (end - start).days + 1

    return [
        (start + timedelta(days=i)).isoformat()
        for i in range(number_of_days)
    ]


def extract_weather(config):
    city = config["city"]

    expected_dates = get_expected_dates(
        config["start_date"],
        config["end_date"],
    )

    params = {
        "latitude": config["latitude"],
        "longitude": config["longitude"],
        "start_date": config["start_date"],
        "end_date": config["end_date"],
        "daily": "temperature_2m_mean,rain_sum",
        "timezone": TIMEZONE,
    }

    print(
        f"\nĐang lấy {city}: "
        f"{config['start_date']} đến {config['end_date']}"
    )

    # 1. Gọi API
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # 2. Lưu raw cùng thông tin về lần lấy dữ liệu
    extracted_at = datetime.now(timezone.utc)

    raw_record = {
        "metadata": {
            "city": city,
            "source": "open-meteo",
            "endpoint": API_URL,
            "request_params": params,
            "extracted_at_utc": extracted_at.isoformat(),
        },
        "response": data,
    }

    run_time = extracted_at.strftime("%Y%m%dT%H%M%S%fZ")

    file_name = (
        f"{config['slug']}_"
        f"{config['start_date']}_{config['end_date']}_"
        f"{run_time}.json"
    )

    output_path = OUTPUT_DIR / file_name

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(raw_record, file, ensure_ascii=False, indent=2)

    print(f"Đã lưu raw: {output_path}")

    # 3. Kiểm tra phản hồi sau khi đã lưu raw
    if data.get("error"):
        raise ValueError(f"API báo lỗi: {data.get('reason')}")

    if data.get("utc_offset_seconds") != 25200:
        raise ValueError("Múi giờ phản hồi không có độ lệch UTC+7.")

    daily = data.get("daily", {})

    dates = daily.get("time", [])
    temperatures = daily.get("temperature_2m_mean", [])
    rainfall = daily.get("rain_sum", [])

    if dates != expected_dates:
        raise ValueError(
            f"Ngày trả về không khớp. "
            f"Cần: {expected_dates}; nhận: {dates}"
        )

    if not (len(dates) == len(temperatures) == len(rainfall)):
        raise ValueError("Các mảng dữ liệu có độ dài khác nhau.")

    missing_dates = [
        day
        for day, temperature, rain in zip(
            dates, temperatures, rainfall
        )
        if temperature is None or rain is None
    ]

    if missing_dates:
        raise ValueError(
            f"Có dữ liệu null tại các ngày: {missing_dates}. "
            "Raw đã được giữ lại để kiểm tra."
        )

    print(f"Kiểm tra đạt: {len(dates)} ngày, không có null.")

    return len(dates)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_days = 0
    failed_cities = []

    for config in CITY_CONFIGS:
        try:
            total_days += extract_weather(config)
        except (requests.RequestException, ValueError, OSError) as error:
            failed_cities.append(config["city"])
            print(f"LỖI tại {config['city']}: {error}")

    print(f"\nTổng số bản ghi thành phố–ngày đạt kiểm tra: {total_days}")

    if failed_cities:
        print(f"Thành phố chưa hoàn tất: {', '.join(failed_cities)}")
        raise SystemExit(1)

    print("Hoàn tất cả 3 thành phố.")


if __name__ == "__main__":
    main()