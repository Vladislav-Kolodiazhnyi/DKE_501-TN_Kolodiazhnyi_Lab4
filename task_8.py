import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils import get_spark_session, read_data, write_data

def run_task_8():
    spark = get_spark_session("Task_8_Top10_By_Genre")

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

    akas_filtered_df = (akas_df
        .filter(F.col('isOriginalTitle') == 1)
        .select(F.col('titleId'), F.col('title'))
    )

    # Об'єднуємо дані
    movies_info = (basics_df
        .filter(F.col('titleType') == 'movie')
        .filter(F.col('isAdult') == 0)
        .filter(F.col('genres').isNotNull())
        .join(ratings_df, on='tconst', how='inner')
        .join(akas_filtered_df, basics_df['tconst'] == F.col('titleId'), how='inner')
        .select(F.col('title'), F.col('genres'), F.col('averageRating'), F.col('numVotes'))
    )

    # Розгортаємо жанри (explode), оскільки в одному полі їх може бути кілька через кому
    movies_exploded = movies_info.withColumn('genre', F.explode(F.split(F.col('genres'), ',')))

    # Віконна функція для ранжування у межах кожного жанру
    window_spec_genre = (Window
        .partitionBy('genre')
        .orderBy(F.col('averageRating').desc(), F.col('numVotes').desc())
    )

    result_t8 = (movies_exploded
        .withColumn('rank_in_genre', F.row_number().over(window_spec_genre))
        .filter(F.col('rank_in_genre') <= 10)
        .select('genre', 'title', 'averageRating', 'numVotes')
        .orderBy('genre', F.col('averageRating').desc(), F.col('numVotes').desc())
    )

    # Load & Show
    result_t8.show(10, truncate=False)
    write_data(result_t8, 'task8_top10_by_genre')

    spark.stop()

if __name__ == "__main__":
    run_task_8()