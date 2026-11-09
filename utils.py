import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as T

# Константи шляхів
DATA_DIR = "imdb-data/"
RESULTS_DIR = "imdb-results/"

# Ініціалізація та налаштування сесії Spark
def get_spark_session(app_name: str = "IMDB_ETL_Task") -> SparkSession:
    return (SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config(conf=SparkConf())
        .getOrCreate())

# Зчитування TSV-файлу IMDB із заданою схемою (Extract)
def read_data(spark_session: SparkSession, file_name: str, schema: T.StructType):
    path = os.path.join(DATA_DIR, file_name)
    df = spark_session.read.csv(
        path,
        header=True,
        sep='\t',
        nullValue=r'\N',
        schema=schema
    )
    return df

# Запис результатів обробки у TSV-файл (Load)
def write_data(df, task_name: str):
    output_dir = os.path.join(RESULTS_DIR, task_name)
    print(f"Збереження результату у: {output_dir}")
    df.write.csv(
        output_dir,
        header=True,
        sep='\t',
        mode='overwrite'
    )