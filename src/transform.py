"""Етап трансформації: загальна інформація, статистика, очищення та бізнес-питання."""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.schemas import JOIN_KEYS

# Межі Нью-Йорка для відсіювання нульових / помилкових координат
NYC_LON = (-74.30, -73.65)
NYC_LAT = (40.45, 40.95)

DAY_NAMES = {1: "Неділя", 2: "Понеділок", 3: "Вівторок", 4: "Середа",
             5: "Четвер", 6: "П'ятниця", 7: "Субота"}


# --------------------------------------------------------------------------
# 1. Загальна інформація про набір даних
# --------------------------------------------------------------------------
def dataset_overview(df: DataFrame, name: str) -> int:
    """Виводить схему, кількість колонок і рядків, типи колонок. Повертає к-сть рядків."""
    print(f"\n{'=' * 70}\nЗагальна інформація: {name}\n{'=' * 70}")
    df.printSchema()
    n_rows = df.count()
    types: Dict[str, int] = {}
    for _, dtype in df.dtypes:
        types[dtype] = types.get(dtype, 0) + 1
    print(f"Кількість колонок: {len(df.columns)}")
    print(f"Кількість рядків:  {n_rows:,}".replace(",", " "))
    print("Колонок за типами: " + ", ".join(f"{t} - {c}" for t, c in types.items()))
    print(f"Кількість розділів (partitions): {df.rdd.getNumPartitions()}")
    return n_rows


def categorical_overview(df: DataFrame, columns) -> None:
    """Розподіл значень категоріальних колонок."""
    for c in columns:
        print(f"\nРозподіл значень колонки {c}:")
        df.groupBy(c).count().orderBy(F.desc("count")).show(15, truncate=False)


# --------------------------------------------------------------------------
# 2. Статистика числових колонок
# --------------------------------------------------------------------------
def numeric_columns(df: DataFrame):
    return [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float")]


def numeric_stats(df: DataFrame, name: str) -> DataFrame:
    """count, mean, stddev, min, квартилі, max для числових колонок."""
    cols = numeric_columns(df)
    print(f"\nСтатистика числових колонок: {name}")
    stats = df.select(cols).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
    # Округлення для читабельності
    rounded = stats.select("summary", *[
        F.format_number(F.col(c).cast("double"), 2).alias(c) for c in cols
    ])
    rounded.show(truncate=False)
    return stats


def anomaly_report(trips: DataFrame, fares: DataFrame) -> None:
    """Підрахунок аномальних значень, які будуть відфільтровані при очищенні."""
    print("\nАномальні значення в trip_data:")
    trips.select(
        F.count(F.lit(1)).alias("всього"),
        F.sum((F.col("trip_distance") <= 0).cast("int")).alias("distance<=0"),
        F.sum((F.col("trip_distance") > 100).cast("int")).alias("distance>100"),
        F.sum((F.col("trip_time_in_secs") <= 0).cast("int")).alias("time<=0"),
        F.sum((F.col("trip_time_in_secs") > 4 * 3600).cast("int")).alias("time>4год"),
        F.sum((~F.col("passenger_count").between(1, 6)).cast("int")).alias("passengers_не_1-6"),
        F.sum((F.col("pickup_longitude") == 0).cast("int")).alias("longitude=0"),
    ).show(truncate=False)
    print("Аномальні значення в trip_fare:")
    fares.select(
        F.count(F.lit(1)).alias("всього"),
        F.sum((F.col("fare_amount") <= 0).cast("int")).alias("fare<=0"),
        F.sum((F.col("fare_amount") > 500).cast("int")).alias("fare>500"),
        F.sum((F.col("tip_amount") < 0).cast("int")).alias("tip<0"),
        F.sum((F.col("total_amount") <= 0).cast("int")).alias("total<=0"),
    ).show(truncate=False)


# --------------------------------------------------------------------------
# Очищення та підготовка
# --------------------------------------------------------------------------
def clean_trips(trips: DataFrame) -> DataFrame:
    """Залишає фізично можливі поїздки в межах Нью-Йорка та додає похідні колонки."""
    return (
        trips
        .filter(F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull())
        .filter(F.col("trip_distance").between(0.01, 100))
        .filter(F.col("trip_time_in_secs").between(30, 4 * 3600))
        .filter(F.col("passenger_count").between(1, 6))
        .filter(F.col("pickup_longitude").between(*NYC_LON) & F.col("pickup_latitude").between(*NYC_LAT))
        .filter(F.col("dropoff_longitude").between(*NYC_LON) & F.col("dropoff_latitude").between(*NYC_LAT))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))  # 1 = неділя
        .withColumn("trip_minutes", F.col("trip_time_in_secs") / 60)
        .withColumn("speed_mph", F.col("trip_distance") / (F.col("trip_time_in_secs") / 3600))
        .filter(F.col("speed_mph") < 80)
    )


def clean_fares(fares: DataFrame) -> DataFrame:
    """Залишає записи з додатною вартістю та невід'ємними чайовими."""
    return (
        fares
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("fare_amount").between(2.5, 500))
        .filter(F.col("tip_amount") >= 0)
        .filter(F.col("total_amount") > 0)
    )


def join_trips_fares(trips: DataFrame, fares: DataFrame) -> DataFrame:
    """Об'єднує поїздки та оплату за ключем (medallion, hack_license, vendor_id, pickup_datetime)."""
    return trips.join(fares, on=JOIN_KEYS, how="inner")


# --------------------------------------------------------------------------
# 3. Бізнес-питання
# --------------------------------------------------------------------------
def q1_jfk_by_hour(spark: SparkSession, trips: DataFrame) -> DataFrame:
    """Q1 (filter, group by, Spark SQL).
    Як залежать кількість, тривалість та середня швидкість поїздок за фіксованим
    тарифом JFK (rate_code = 2) від години доби?
    """
    trips.createOrReplaceTempView("trips")
    return spark.sql("""
        SELECT pickup_hour                                   AS hour,
               COUNT(*)                                      AS trips,
               ROUND(AVG(trip_minutes), 1)                   AS avg_minutes,
               ROUND(AVG(trip_distance), 2)                  AS avg_miles,
               ROUND(SUM(trip_distance) / (SUM(trip_time_in_secs) / 3600), 1) AS avg_speed_mph
        FROM trips
        WHERE rate_code = 2
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)


def q2_tip_pct_by_passengers(joined: DataFrame) -> DataFrame:
    """Q2 (join, filter, group by).
    Який середній відсоток чайових при оплаті карткою залежно від кількості пасажирів?
    """
    return (
        joined
        .filter(F.col("payment_type") == "CRD")
        .withColumn("tip_pct", F.col("tip_amount") / F.col("fare_amount") * 100)
        .groupBy("passenger_count")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.percentile_approx("tip_pct", 0.5), 2).alias("median_tip_pct"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
        )
        .orderBy("passenger_count")
    )


def q3_top_drivers_by_weekday(fares: DataFrame, top_n: int = 3) -> DataFrame:
    """Q3 (group by, window function).
    Які 3 водії (hack_license) заробили найбільше в кожен день тижня?
    """
    per_driver = (
        fares
        .withColumn("dow", F.dayofweek("pickup_datetime"))
        .groupBy("dow", "hack_license")
        .agg(F.count(F.lit(1)).alias("trips"),
             F.round(F.sum("total_amount"), 2).alias("revenue"))
    )
    w = Window.partitionBy("dow").orderBy(F.desc("revenue"))
    day_name = F.create_map(*[x for k, v in DAY_NAMES.items() for x in (F.lit(k), F.lit(v))])
    return (
        per_driver
        .withColumn("rank", F.dense_rank().over(w))
        .filter(F.col("rank") <= top_n)
        .withColumn("day", day_name[F.col("dow")])
        .select("dow", "day", "rank", F.substring("hack_license", 1, 10).alias("driver"),
                "trips", "revenue")
        .orderBy("dow", "rank")
    )


def q4_daily_trend(trips: DataFrame) -> DataFrame:
    """Q4 (group by, window functions).
    Як змінювалася щоденна кількість поїздок: ковзне середнє за 7 днів,
    зміна відносно попереднього дня та ранг дня за завантаженістю.
    """
    daily = trips.groupBy("pickup_date").agg(
        F.count(F.lit(1)).alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
    )
    w_order = Window.orderBy("pickup_date")
    w_7d = w_order.rowsBetween(-6, 0)
    return (
        daily
        .withColumn("moving_avg_7d", F.round(F.avg("trips").over(w_7d), 0))
        .withColumn("prev_day", F.lag("trips").over(w_order))
        .withColumn("change_pct",
                    F.round((F.col("trips") - F.col("prev_day")) / F.col("prev_day") * 100, 1))
        .withColumn("busy_rank", F.rank().over(Window.orderBy(F.desc("trips"))))
        .drop("prev_day")
        .orderBy("pickup_date")
    )


def q5_most_expensive_per_mile(joined: DataFrame, limit: int = 10) -> DataFrame:
    """Q5 (join, filter).
    Які 10 поїздок довше 1 милі, оплачених готівкою, мали найвищу вартість за милю?
    """
    return (
        joined
        .filter((F.col("payment_type") == "CSH") & (F.col("trip_distance") > 1))
        .withColumn("usd_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
        .select("pickup_datetime", "rate_code", "trip_distance", "trip_minutes",
                "fare_amount", "total_amount", "usd_per_mile")
        .withColumn("trip_minutes", F.round("trip_minutes", 1))
        .orderBy(F.desc("usd_per_mile"))
        .limit(limit)
    )


def q6_weekend_distance_buckets(joined: DataFrame) -> DataFrame:
    """Q6 (join, filter, group by).
    Як у вихідні розподіляються поїздки за діапазонами відстані:
    кількість, середня вартість, середні чайові та частка оплати карткою?
    """
    bucket = (F.when(F.col("trip_distance") < 1, "1) < 1 милі")
              .when(F.col("trip_distance") < 3, "2) 1-3 милі")
              .when(F.col("trip_distance") < 5, "3) 3-5 миль")
              .when(F.col("trip_distance") < 10, "4) 5-10 миль")
              .otherwise("5) 10+ миль"))
    return (
        joined
        .filter(F.col("pickup_dow").isin(1, 7))  # неділя, субота
        .withColumn("distance_bucket", bucket)
        .groupBy("distance_bucket")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(F.avg((F.col("payment_type") == "CRD").cast("double")) * 100, 1).alias("card_share_pct"),
        )
        .orderBy("distance_bucket")
    )


BUSINESS_QUESTIONS = [
    ("q1_jfk_by_hour", "Q1. Поїздки за тарифом JFK по годинах доби (filter, group by, SQL)"),
    ("q2_tip_pct_by_passengers", "Q2. Відсоток чайових при оплаті карткою за кількістю пасажирів (join, filter, group by)"),
    ("q3_top_drivers_by_weekday", "Q3. Топ-3 водії за виручкою в кожен день тижня (group by, window)"),
    ("q4_daily_trend", "Q4. Щоденна динаміка поїздок: ковзне середнє, зміна, ранг (group by, window)"),
    ("q5_most_expensive_per_mile", "Q5. Топ-10 найдорожчих за милю поїздок готівкою (join, filter)"),
    ("q6_weekend_distance_buckets", "Q6. Поїздки у вихідні за діапазонами відстані (join, filter, group by)"),
]


def run_business_questions(spark: SparkSession, trips: DataFrame,
                           fares: DataFrame) -> Dict[str, DataFrame]:
    """Обчислює всі 6 бізнес-питань і повертає словник {назва: DataFrame}."""
    joined = join_trips_fares(trips, fares).persist()
    results = {
        "q1_jfk_by_hour": q1_jfk_by_hour(spark, trips),
        "q2_tip_pct_by_passengers": q2_tip_pct_by_passengers(joined),
        "q3_top_drivers_by_weekday": q3_top_drivers_by_weekday(fares),
        "q4_daily_trend": q4_daily_trend(trips),
        "q5_most_expensive_per_mile": q5_most_expensive_per_mile(joined),
        "q6_weekend_distance_buckets": q6_weekend_distance_buckets(joined),
    }
    return results
