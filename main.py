import os
import sys
from pyspark.sql import SparkSession

# Вказуємо PySpark використовувати саме інтерпретатор з активного venv
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    # Ініціалізація Spark з явним зазначенням адреси 127.0.0.1
    spark = SparkSession.builder \
        .appName("IMDb Spark Project") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    print("PySpark успішно ініціалізовано!")

    # Тестовий DataFrame
    data = [
        ("The Shawshank Redemption", 1994, 9.3),
        ("The Godfather", 1972, 9.2),
        ("The Dark Knight", 2008, 9.0)
    ]
    columns = ["Title", "ReleaseYear", "Rating"]

    df = spark.createDataFrame(data, schema=columns)
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()