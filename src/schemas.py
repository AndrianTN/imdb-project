"""Схеми (StructType) для набору даних NYC Taxi Trip Data 2013.

Набір складається з двох груп CSV-файлів (по одному на кожен місяць 2013 року):
    * trip_data_<N>.csv - інформація про поїздку (час, відстань, координати);
    * trip_fare_<N>.csv - інформація про оплату поїздки (тариф, чайові, сума).

Обидві таблиці пов'язуються між собою за ключем
(medallion, hack_license, vendor_id, pickup_datetime).

Схеми задаються явно, а не через inferSchema, тому що:
    1) inferSchema змушує Spark зайвий раз прочитати десятки гігабайт даних;
    2) явні типи гарантують однаковий результат для всіх місяців;
    3) заголовки в оригінальних файлах містять пробіли на початку назв
       (" hack_license"), а явна схема дає чисті назви колонок.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Ключові колонки, спільні для обох таблиць (використовуються для join)
JOIN_KEYS = ["medallion", "hack_license", "vendor_id", "pickup_datetime"]

# trip_data_<N>.csv - 14 колонок
TRIP_DATA_SCHEMA = StructType([
    StructField("medallion", StringType(), True),            # хеш номера таксі
    StructField("hack_license", StringType(), True),         # хеш ліцензії водія
    StructField("vendor_id", StringType(), True),            # постачальник (CMT / VTS)
    StructField("rate_code", IntegerType(), True),           # код тарифу (1 - стандартний, 2 - JFK, ...)
    StructField("store_and_fwd_flag", StringType(), True),   # Y/N - запис зберігався в машині без зв'язку
    StructField("pickup_datetime", TimestampType(), True),   # час посадки
    StructField("dropoff_datetime", TimestampType(), True),  # час висадки
    StructField("passenger_count", IntegerType(), True),     # кількість пасажирів
    StructField("trip_time_in_secs", IntegerType(), True),   # тривалість поїздки, с
    StructField("trip_distance", DoubleType(), True),        # відстань, милі
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
])

# trip_fare_<N>.csv - 11 колонок
TRIP_FARE_SCHEMA = StructType([
    StructField("medallion", StringType(), True),
    StructField("hack_license", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("payment_type", StringType(), True),         # CSH, CRD, NOC, DIS, UNK
    StructField("fare_amount", DoubleType(), True),          # тариф за лічильником, $
    StructField("surcharge", DoubleType(), True),            # нічна / година-пік надбавка, $
    StructField("mta_tax", DoubleType(), True),              # податок MTA, $
    StructField("tip_amount", DoubleType(), True),           # чайові, $
    StructField("tolls_amount", DoubleType(), True),         # платні дороги / мости, $
    StructField("total_amount", DoubleType(), True),         # загальна сума, $
])
