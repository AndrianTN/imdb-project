"""Етап видобування: зчитування CSV-файлів NYC Taxi у DataFrame за схемами."""

from typing import Iterable, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.config import DATA_DIR, TRIP_DATA_PREFIX, TRIP_FARE_PREFIX
from src.schemas import TRIP_DATA_SCHEMA, TRIP_FARE_SCHEMA


def _file_glob(prefix: str, months: Optional[Iterable[int]] = None) -> str:
    """Будує шаблон імені файлу, наприклад 'trip_data_{1,2}.csv'.

    Args:
        prefix: 'trip_data' або 'trip_fare'.
        months: номери місяців (1-12). None - усі наявні файли.
    """
    if not months:
        return f"{prefix}_*.csv"
    months = sorted(set(int(m) for m in months))
    for m in months:
        if not 1 <= m <= 12:
            raise ValueError(f"Номер місяця має бути від 1 до 12, отримано: {m}")
    if len(months) == 1:
        return f"{prefix}_{months[0]}.csv"
    return f"{prefix}_{{{','.join(str(m) for m in months)}}}.csv"


def read_csv_with_schema(spark: SparkSession, data_dir: str, file_glob: str,
                         schema: StructType) -> DataFrame:
    """Зчитує CSV-файли з папки (включно з підпапками) за заданою схемою.

    Args:
        spark: активна SparkSession.
        data_dir: папка з даними.
        file_glob: шаблон імені файлів (pathGlobFilter).
        schema: схема StructType.

    Returns:
        DataFrame з колонками та типами зі схеми.
    """
    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", True)                    # перший рядок кожного файлу - заголовок
        .option("ignoreLeadingWhiteSpace", True)   # у файлах є пробіли після коми
        .option("ignoreTrailingWhiteSpace", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("mode", "PERMISSIVE")              # некоректні значення -> null, а не падіння
        .option("recursiveFileLookup", True)       # файли можуть лежати в підпапці після 7-Zip
        .option("pathGlobFilter", file_glob)
        .load(data_dir)
    )


def extract_trip_data(spark: SparkSession, data_dir: str = DATA_DIR,
                      months: Optional[Iterable[int]] = None) -> DataFrame:
    """Зчитує файли trip_data_<N>.csv (інформація про поїздки)."""
    return read_csv_with_schema(
        spark, data_dir, _file_glob(TRIP_DATA_PREFIX, months), TRIP_DATA_SCHEMA
    )


def extract_trip_fare(spark: SparkSession, data_dir: str = DATA_DIR,
                      months: Optional[Iterable[int]] = None) -> DataFrame:
    """Зчитує файли trip_fare_<N>.csv (інформація про оплату)."""
    return read_csv_with_schema(
        spark, data_dir, _file_glob(TRIP_FARE_PREFIX, months), TRIP_FARE_SCHEMA
    )


def extract_all(spark: SparkSession, data_dir: str = DATA_DIR,
                months: Optional[Iterable[int]] = None,
                include_fares: bool = True) -> Tuple[DataFrame, Optional[DataFrame]]:
    """Зчитує обидві частини набору даних.

    Returns:
        (trip_data_df, trip_fare_df). Якщо include_fares=False, другий елемент - None.
    """
    trips = extract_trip_data(spark, data_dir, months)
    fares = extract_trip_fare(spark, data_dir, months) if include_fares else None
    return trips, fares


def check_dataframe(df: DataFrame, name: str, n_rows: int = 5,
                    count_rows: bool = True) -> None:
    """Перевіряє, що DataFrame зчитався коректно, і виводить результат у консоль.

    Виконує: printSchema, show, підрахунок колонок і рядків,
    підрахунок null у кожній колонці (null з'являються, якщо значення
    не вдалося привести до типу зі схеми).
    """
    print(f"\n{'=' * 70}\n{name}\n{'=' * 70}")
    files = df.inputFiles()
    print(f"Файлів зчитано: {len(files)}")
    for path in files:
        print(f"  - {path}")
    if not files:
        print("УВАГА: не знайдено жодного файлу. Перевірте шлях до даних.")
        return

    print("\nСхема:")
    df.printSchema()

    print(f"Перші {n_rows} рядків:")
    df.show(n_rows, truncate=False)

    print(f"Кількість колонок: {len(df.columns)}")
    if count_rows:
        # Один прохід по даних: загальна кількість рядків + null у кожній колонці
        aggs = [F.count(F.lit(1)).alias("__total__")] + [
            F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
        ]
        stats = df.agg(*aggs).collect()[0].asDict()
        total = stats.pop("__total__")
        print(f"Кількість рядків: {total:,}".replace(",", " "))
        print("Кількість null-значень по колонках:")
        for col_name, nulls in stats.items():
            share = (nulls / total * 100) if total else 0
            print(f"  {col_name:<20} {nulls:>12,}  ({share:.4f}%)".replace(",", " "))
