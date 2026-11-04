"""Етап налаштування: перевірка робочого середовища для PySpark.

Запуск (з активованим venv, у корені проєкту):
    python check_env.py
"""

import os
import platform
import shutil
import subprocess
import sys


def line(title: str, value: str) -> None:
    print(f"  {title:<28} {value}")


def java_version() -> str:
    java = shutil.which("java")
    if not java:
        return "НЕ ЗНАЙДЕНО (встановіть Java 17 або 21)"
    out = subprocess.run([java, "-version"], capture_output=True, text=True)
    return (out.stderr or out.stdout).splitlines()[0]


def main() -> None:
    print("=" * 70)
    print("Перевірка середовища")
    print("=" * 70)
    line("ОС:", f"{platform.system()} {platform.release()}")
    line("Python:", sys.version.split()[0])
    line("Інтерпретатор:", sys.executable)
    line("Java:", java_version())
    line("JAVA_HOME:", os.environ.get("JAVA_HOME", "не задано"))

    hadoop_home = os.environ.get("HADOOP_HOME")
    line("HADOOP_HOME:", hadoop_home or "не задано")
    if platform.system() == "Windows":
        for f in ("winutils.exe", "hadoop.dll"):
            ok = bool(hadoop_home) and os.path.exists(os.path.join(hadoop_home, "bin", f))
            line(f"  bin\\{f}:", "знайдено" if ok else "НЕ ЗНАЙДЕНО")

    try:
        import pyspark
        line("PySpark:", pyspark.__version__)
    except ImportError:
        line("PySpark:", "НЕ ВСТАНОВЛЕНО (pip install -r requirements.txt)")
        return

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("Env check").master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    line("Spark:", spark.version)
    line("Ядер для local[*]:", str(spark.sparkContext.defaultParallelism))

    print("\nТестовий DataFrame:")
    df = spark.createDataFrame(
        [("CMT", 1, 2.5, 10.0), ("VTS", 2, 7.1, 24.5), ("CMT", 1, 0.9, 5.5)],
        ["vendor_id", "passenger_count", "trip_distance", "fare_amount"],
    )
    df.show()
    df.groupBy("vendor_id").avg("fare_amount").show()
    spark.stop()
    print("Середовище налаштоване коректно!")


if __name__ == "__main__":
    main()
