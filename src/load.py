"""Етап запису результатів: збереження DataFrame у CSV."""

import os
from typing import Dict

from pyspark.sql import DataFrame


def write_csv(df: DataFrame, path: str) -> None:
    """Записує DataFrame в одну CSV-частину з заголовком (папка path/part-*.csv)."""
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .option("encoding", "UTF-8")
       # Без цього час записується як 2013-01-01T04:44:49.000Z, тобто
       # виглядає як UTC, хоча в даних це місцевий час Нью-Йорка
       .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
       .csv(path))


def write_results(results: Dict[str, DataFrame], output_dir: str) -> None:
    """Записує кожне бізнес-питання в окрему папку output_dir/<назва>/."""
    for name, df in results.items():
        path = os.path.join(output_dir, name)
        write_csv(df, path)
        print(f"  записано: {path}")
