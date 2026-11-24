"""Точка входу проєкту NYC Taxi Spark Project.

Запуск (з активованим venv, у корені проєкту):
    python main.py extract   --months 1        # етап видобування
    python main.py transform --months 1        # етап трансформації (опис, статистика, 6 питань)
    python main.py write     --months 1        # етап запису відповідей у output/*.csv

Додаткові параметри:
    --sample 0.1        працювати з 10% даних (швидка перевірка)
    --no-count          (extract) не рахувати рядки та null
    --data-dir PATH     інша папка з даними
"""

import argparse
import time

from src.config import DATA_DIR, OUTPUT_DIR, create_spark_session
from src.extract import check_dataframe, extract_all


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi Trip Data 2013 - PySpark")
    parser.add_argument("stage", nargs="?", default="extract",
                        choices=["extract", "transform", "write"],
                        help="етап проєкту (за замовчуванням extract)")
    parser.add_argument("--data-dir", default=DATA_DIR, help="папка з розпакованими CSV")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, help="папка для результатів")
    parser.add_argument("--months", type=int, nargs="*",
                        help="номери місяців 1-12 (за замовчуванням - усі)")
    parser.add_argument("--sample", type=float, default=None,
                        help="частка даних для швидкої перевірки, напр. 0.1")
    parser.add_argument("--no-count", action="store_true",
                        help="(extract) не рахувати рядки та null")
    parser.add_argument("--no-fares", action="store_true",
                        help="(extract) не зчитувати trip_fare")
    return parser.parse_args()


def run_extract(spark, args) -> None:
    trips, fares = extract_all(spark, args.data_dir, args.months,
                               include_fares=not args.no_fares)
    check_dataframe(trips, "trip_data (поїздки)", count_rows=not args.no_count)
    if fares is not None:
        check_dataframe(fares, "trip_fare (оплата)", count_rows=not args.no_count)


def load_clean(spark, args):
    """Видобування + очищення (спільне для transform і write)."""
    from src.transform import clean_fares, clean_trips
    trips, fares = extract_all(spark, args.data_dir, args.months)
    if args.sample:
        trips = trips.sample(fraction=args.sample, seed=42)
        fares = fares.sample(fraction=args.sample, seed=42)
    return trips, fares, clean_trips(trips).persist(), clean_fares(fares).persist()


def run_transform(spark, args) -> None:
    from src.transform import (BUSINESS_QUESTIONS, anomaly_report, categorical_overview,
                               dataset_overview, numeric_stats, run_business_questions)
    trips_raw, fares_raw, trips, fares = load_clean(spark, args)

    # 1. Загальна інформація
    n_trips = dataset_overview(trips_raw, "trip_data")
    n_fares = dataset_overview(fares_raw, "trip_fare")
    categorical_overview(trips_raw, ["vendor_id", "rate_code", "store_and_fwd_flag", "passenger_count"])
    categorical_overview(fares_raw, ["payment_type"])

    # 2. Статистика числових колонок
    numeric_stats(trips_raw, "trip_data")
    numeric_stats(fares_raw, "trip_fare")
    anomaly_report(trips_raw, fares_raw)

    n_trips_clean, n_fares_clean = trips.count(), fares.count()
    print(f"\nПісля очищення: trip_data {n_trips_clean:,} з {n_trips:,} "
          f"({n_trips_clean / n_trips * 100:.2f}%), "
          f"trip_fare {n_fares_clean:,} з {n_fares:,} ({n_fares_clean / n_fares * 100:.2f}%)"
          .replace(",", " "))

    # 3. Бізнес-питання
    results = run_business_questions(spark, trips, fares)
    for key, title in BUSINESS_QUESTIONS:
        print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
        start = time.time()
        results[key].show(40, truncate=False)
        print(f"(час виконання: {time.time() - start:.1f} c)")


def run_write(spark, args) -> None:
    from src.load import write_results
    from src.transform import run_business_questions
    _, _, trips, fares = load_clean(spark, args)
    results = run_business_questions(spark, trips, fares)
    print(f"Запис відповідей на бізнес-питання у {args.output_dir}:")
    write_results(results, args.output_dir)


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    print(f"PySpark {spark.version} успішно ініціалізовано!")
    print(f"Етап: {args.stage} | папка з даними: {args.data_dir} | місяці: {args.months or 'усі'}"
          + (f" | вибірка: {args.sample}" if args.sample else ""))
    start = time.time()
    {"extract": run_extract, "transform": run_transform, "write": run_write}[args.stage](spark, args)
    print(f"\nЗагальний час: {time.time() - start:.1f} c")
    spark.stop()


if __name__ == "__main__":
    main()
