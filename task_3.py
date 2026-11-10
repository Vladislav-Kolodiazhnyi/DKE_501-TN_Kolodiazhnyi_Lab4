import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import get_spark_session, read_data, write_data

def run_task_3():
    spark = get_spark_session("Task_3_Long_Movies")

    # Схема для title.basics.tsv.gz
    schema_basics = T.StructType([
        T.StructField('tconst', T.StringType(), False),
        T.StructField('titleType', T.StringType(), True),
        T.StructField('primaryTitle', T.StringType(), True),
        T.StructField('originalTitle', T.StringType(), True),
        T.StructField('isAdult', T.IntegerType(), True),
        T.StructField('startYear', T.IntegerType(), True),
        T.StructField('endYear', T.IntegerType(), True),
        T.StructField('runtimeMinutes', T.IntegerType(), True),
        T.StructField('genres', T.StringType(), True)
    ])

    # Схема для title.akas.tsv.gz
    schema_akas = T.StructType([
        T.StructField('titleId', T.StringType(), False),
        T.StructField('ordering', T.IntegerType(), False),
        T.StructField('title', T.StringType(), True),
        T.StructField('region', T.StringType(), True),
        T.StructField('language', T.StringType(), True),
        T.StructField('types', T.StringType(), True),
        T.StructField('attributes', T.StringType(), True),
        T.StructField('isOriginalTitle', T.IntegerType(), True)
    ])

    # Extract
    basics_df = read_data(spark, 'title.basics.tsv.gz', schema_basics)
    akas_df = read_data(spark, 'title.akas.tsv.gz', schema_akas)

    # Transform
    # Відбираємо фільми з тривалістю понад 120 хвилин
    long_movies = (basics_df
        .filter((F.col('titleType') == 'movie') & (F.col('runtimeMinutes') > 120))
        .select(F.col('tconst'), F.col('runtimeMinutes')))

    # Відбираємо лише оригінальні назви для коректного відображення
    original_titles = (akas_df
        .filter(F.col('isOriginalTitle') == 1)
        .select(F.col('titleId'), F.col('title')))

    # Join за ключем ідентифікатора фільму
    result_t3 = (long_movies
        .join(original_titles, long_movies['tconst'] == original_titles['titleId'], how='inner')
        .filter(F.col('title').isNotNull())
        .select(
            F.col('title').alias('movie_title'),
            F.col('runtimeMinutes').alias('duration_minutes')
        )
        .distinct())

    # Load & Show
    result_t3.show(5, truncate=False)
    write_data(result_t3, 'task3_long_movies')

    spark.stop()

if __name__ == "__main__":
    run_task_3()