import json
from datetime import date, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "weather"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "weather"

# Thay bằng tên đầy đủ của ba file raw bạn vừa upload.
INPUT_FILES = [
    "hanoi_2026-10-01_2026-10-19_20261020T071402819979Z.json",
    "da_nang_2026-10-02_2026-10-06_20261020T071404428179Z.json",
    "ho_chi_minh_city_2026-10-02_2026-10-05_20261020T071405470012Z.json",
]

# Phạm vi đã chốt từ dữ liệu đơn hàng.
EXPECTED_RANGES = {
    "Hanoi": ("2026-10-01", "2026-10-19"),
    "Da Nang": ("2026-10-02", "2026-10-06"),
    "Ho Chi Minh City": ("2026-10-02", "2026-10-05"),
}


def get_expected_dates(start_text, end_text):
    start = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)

    return [
        (start + timedelta(days=i)).isoformat()
        for i in range((end - start).days + 1)
    ]


def transform_file(file_name):
    input_path = RAW_DIR / file_name

    with input_path.open("r", encoding="utf-8") as file:
        raw = json.load(file)

    metadata = raw["metadata"]
    response = raw["response"]
    city = metadata["city"]

    if city not in EXPECTED_RANGES:
        raise ValueError(f"Thành phố ngoài phạm vi: {city}")

    if response.get("error"):
        raise ValueError(f"API báo lỗi trong file: {file_name}")

    if response.get("timezone") != "Asia/Ho_Chi_Minh":
        raise ValueError(f"{city}: múi giờ không đúng.")

    units = response["daily_units"]

    if (
        units.get("temperature_2m_mean") != "°C"
        or units.get("rain_sum") != "mm"
    ):
        raise ValueError(f"{city}: đơn vị đo không đúng.")

    daily = response["daily"]
    dates = daily["time"]
    temperatures = daily["temperature_2m_mean"]
    rainfall = daily["rain_sum"]

    start_date, end_date = EXPECTED_RANGES[city]
    expected_dates = get_expected_dates(start_date, end_date)

    if dates != expected_dates:
        raise ValueError(f"{city}: danh sách ngày không đúng phạm vi.")

    if not (len(dates) == len(temperatures) == len(rainfall)):
        raise ValueError(f"{city}: các mảng có độ dài khác nhau.")

    records = []

    for day, temperature, rain in zip(dates, temperatures, rainfall):
        # bool cũng được Python xem là kiểu số, nên loại riêng.
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            for value in (temperature, rain)
        ):
            raise ValueError(f"{city}, {day}: giá trị thiếu hoặc không phải số.")

        if rain < 0:
            raise ValueError(f"{city}, {day}: lượng mưa âm.")

        records.append({
            "city": city,
            "weather_date": day,
            "temperature_mean_c": temperature,
            "rain_sum_mm": rain,
            "source": metadata["source"],
            "extracted_at_utc": metadata["extracted_at_utc"],
            "source_file": file_name,
        })

    print(f"{city}: chuẩn hóa {len(records)} bản ghi.")
    return records


def main():
    all_records = []
    seen_keys = set()

    for file_name in INPUT_FILES:
        records = transform_file(file_name)

        for record in records:
            key = (record["city"], record["weather_date"])

            if key in seen_keys:
                raise ValueError(f"Trùng thành phố và ngày: {key}")

            seen_keys.add(key)
            all_records.append(record)

    expected_keys = {
        (city, day)
        for city, (start, end) in EXPECTED_RANGES.items()
        for day in get_expected_dates(start, end)
    }

    if seen_keys != expected_keys:
        raise ValueError("Dữ liệu chưa đủ các thành phố và ngày cần thiết.")

    all_records.sort(
        key=lambda record: (record["city"], record["weather_date"])
    )

    # Chỉ ghi kết quả sau khi toàn bộ dữ liệu đã đạt kiểm tra.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "weather_daily.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(all_records, file, ensure_ascii=False, indent=2)

    print(f"\nTổng: {len(all_records)} bản ghi.")
    print("Không trùng city + weather_date.")
    print(f"Đã lưu: {output_path}")


if __name__ == "__main__":
    main()