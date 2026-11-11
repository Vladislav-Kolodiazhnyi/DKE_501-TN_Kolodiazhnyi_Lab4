import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils import get_spark_session, read_data, write_data

def run_task_7():
    spark = get_spark_session("Task_7_Top10_By_Decade")

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

    # Схема для title.ratings.tsv.gz
    schema_ratings = T.StructType([
        T.StructField('tconst', T.StringType(), False),
        T.StructField('averageRating', T.FloatType(), True),
        T.StructField('numVotes', T.IntegerType(), True)
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
    ratings_df = read_data(spark, 'title.ratings.tsv.gz', schema_ratings)
    akas_df = read_data(spark, 'title.akas.tsv.gz', schema_akas)

    # Фільтрація оригінальних назв
    akas_filtered_df = (akas_df
        .filter(F.col('isOriginalTitle') == 1)
        .select(F.col('titleId'), F.col('title'))
    )

    # Трансформація та об'єднання датасетів
    movies_info = (basics_df
        .filter(F.col('titleType') == 'movie')
        .filter(F.col('isAdult') == 0)
        .join(ratings_df, on='tconst', how='inner')
        .join(akas_filtered_df, basics_df['tconst'] == F.col('titleId'), how='inner')
        .select(F.col('title'), F.col('startYear'), F.col('averageRating'), F.col('numVotes'))
    )

    # Додаємо розрахунок десятиліття
    movies_info = (movies_info
        .withColumn('Decade', (F.floor(F.col('startYear') / 10) * 10).cast(T.IntegerType()))
        .filter(F.col('Decade').isNotNull())
    )

    # Віконна функція для ранжування у межах десятиліття
    window_spec_decade = (Window
        .partitionBy('Decade')
        .orderBy(F.col('averageRating').desc(), F.col('numVotes').desc())
    )

    result_t7 = (movies_info
        .withColumn('rank_in_decade', F.row_number().over(window_spec_decade))
        .filter(F.col('rank_in_decade') <= 10)
        .select('title', 'Decade', 'averageRating', 'numVotes')
        .orderBy('Decade', F.col('averageRating').desc(), F.col('numVotes').desc())
    )

    # Load & Show
    result_t7.show(10, truncate=False)
    write_data(result_t7, 'task7_top10_by_decade')

    spark.stop()

if __name__ == "__main__":
    run_task_7()