import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import get_spark_session, read_data, write_data

def run_task_4():
    spark = get_spark_session("Task_4_Actors_And_Roles")

    # Схема для title.principals.tsv.gz
    schema_principals = T.StructType([
        T.StructField('tconst', T.StringType(), False),
        T.StructField('ordering', T.IntegerType(), False),
        T.StructField('nconst', T.StringType(), False),
        T.StructField('category', T.StringType(), True),
        T.StructField('job', T.StringType(), True),
        T.StructField('characters', T.StringType(), True)
    ])

    # Схема для name.basics.tsv.gz
    schema_names = T.StructType([
        T.StructField('nconst', T.StringType(), False),
        T.StructField('primaryName', T.StringType(), True),
        T.StructField('birthYear', T.IntegerType(), True),
        T.StructField('deathYear', T.IntegerType(), True),
        T.StructField('primaryProfession', T.StringType(), True),
        T.StructField('knownForTitles', T.StringType(), True)
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
    principals_df = read_data(spark, 'title.principals.tsv.gz', schema_principals)
    names_df = read_data(spark, 'name.basics.tsv.gz', schema_names)
    akas_df = read_data(spark, 'title.akas.tsv.gz', schema_akas)

    # Transform
    # Фільтруємо ролі тільки акторів та актрис, де вказано персонажа
    actors = (principals_df
        .filter(
            F.col('category').isin('actor', 'actress') &
            F.col('characters').isNotNull()
        )
        .select('tconst', 'nconst', 'characters')
        .limit(5000))

    # Отримуємо імена акторів
    names = (names_df.select(F.col('nconst'), F.col('primaryName').alias('actor_name'))
        .limit(5000))

    # Отримуємо оригінальні назви фільмів/серіалів
    titles = (akas_df
        .filter(F.col('isOriginalTitle') == 1)
        .select(F.col('titleId'), F.col('title').alias('movie_title'))
        .limit(5000))

    # Об'єднання таблиць (JOIN)
    result_t4 = (actors
        .join(names, on='nconst', how='inner')
        .join(titles, actors['tconst'] == titles['titleId'], how='inner')
        .select(
            F.col('actor_name'),
            F.col('movie_title'),
            F.col('characters').alias('character_played')
        )
        .distinct())

    # Load & Show
    result_t4.show(5, truncate=False)
    write_data(result_t4, 'task4_actors_and_characters')

    spark.stop()

if __name__ == "__main__":
    run_task_4()