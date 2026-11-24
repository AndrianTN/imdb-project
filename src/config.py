"""Налаштування проєкту: шляхи до даних та створення SparkSession."""

import os
import sys

from pyspark.sql import SparkSession

# Папка з розпакованими CSV. Знаходиться ПОЗА проєктом (вимога завдання).
# Можна перевизначити змінною середовища TAXI_DATA_DIR або аргументом --data-dir.
DATA_DIR = os.environ.get(
    "TAXI_DATA_DIR", r"D:\4corsepoliteh\taxidata\nycTaxiTripData2013"
)

# Папка для результатів (додана в .gitignore)
OUTPUT_DIR = os.environ.get("TAXI_OUTPUT_DIR", "output")

# Тимчасові файли Spark (shuffle, кеш на диску). Виносимо на диск D:,
# бо на системному диску C: мало вільного місця.
SPARK_TMP_DIR = os.environ.get("SPARK_TMP_DIR", r"D:\4corsepoliteh\spark-tmp")

# Шаблони імен файлів
TRIP_DATA_PREFIX = "trip_data"
TRIP_FARE_PREFIX = "trip_fare"


def create_spark_session(app_name: str = "NYC Taxi Spark Project",
                         driver_memory: str = "4g") -> SparkSession:
    """Створює локальну SparkSession.

    Args:
        app_name: назва застосунку у Spark UI.
        driver_memory: пам'ять для драйвера (локально це вся пам'ять Spark).

    Returns:
        SparkSession, налаштована для роботи на одному ПК.
    """
    # PySpark має використовувати інтерпретатор з активного venv
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", driver_memory)
        .config("spark.local.dir", SPARK_TMP_DIR)
        # 200 розділів за замовчуванням - забагато для одного ПК
        .config("spark.sql.shuffle.partitions", "64")
        # Час у файлах - локальний час Нью-Йорка без зони. UTC у сесії
        # гарантує, що значення не зсунуться при переході на літній час.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
