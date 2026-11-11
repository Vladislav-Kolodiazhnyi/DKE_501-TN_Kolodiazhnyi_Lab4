import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils import get_spark_session, read_data, write_data

def run_task_6():
    spark = get_spark_session("Task_6_Top_Series_Episodes")

    # Схема для title.episode.tsv.gz
    schema_episode = T.StructType([
        T.StructField('tconst', T.StringType(), False),
        T.StructField('parentTconst', T.StringType(), True),
        T.StructField('seasonNumber', T.IntegerType(), True),
        T.StructField('episodeNumber', T.IntegerType(), True)
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
    episode_df = read_data(spark, 'title.episode.tsv.gz', schema_episode)
    akas_df = read_data(spark, 'title.akas.tsv.gz', schema_akas)

    # Transform
    # Знаходимо максимальний номер епізоду для кожного серіалу (parentTconst)
    series_episodes_count = (episode_df
        .filter(F.col('parentTconst').isNotNull())
        .groupBy('parentTconst')
        .agg(F.max('episodeNumber').alias('total_episodes'))
    )

    # Отримуємо оригінальні назви серіалів
    original_titles = (akas_df
        .filter(F.col('isOriginalTitle') == 1)
        .select(F.col('titleId'), F.col('title').alias('series_title'))
    )

    # Об'єднуємо та сортуємо для отримання топ-50
    result_t6 = (series_episodes_count
        .join(original_titles, series_episodes_count['parentTconst'] == original_titles['titleId'], how='inner')
        .select(
            F.col('series_title'),
            F.col('total_episodes')
        )
        .orderBy(F.col('total_episodes').desc())
        .limit(50)
    )

    # Load & Show
    result_t6.show(10, truncate=False)
    write_data(result_t6, 'task6_top50_series_episodes')

    spark.stop()

if __name__ == "__main__":
    run_task_6()