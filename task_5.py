import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import get_spark_session, read_data, write_data

def run_task_5():
    spark = get_spark_session("Task_5_Adult_Movies_By_Region")

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
    # Фільтруємо дорослі фільми/серіали (isAdult == 1)
    adult_basics = (basics_df
        .filter(F.col('isAdult') == 1)
        .select('tconst')
        .limit(10000))

    # Об'єднуємо з akas для отримання регіонів і групуємо
    result_t5 = (adult_basics
        .join(akas_df, adult_basics['tconst'] == akas_df['titleId'], how='inner')
        .filter(F.col('region').isNotNull())
        .groupBy('region')
        .agg(F.count('tconst').alias('adult_titles_count'))
        .orderBy(F.col('adult_titles_count').desc())
        .limit(100))

    # Load & Show
    result_t5.show(10, truncate=False)
    write_data(result_t5, 'task5_top100_adult_regions')

    spark.stop()

if __name__ == "__main__":
    run_task_5()