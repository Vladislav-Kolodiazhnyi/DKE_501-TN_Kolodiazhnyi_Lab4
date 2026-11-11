import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import get_spark_session, read_data, write_data

def run_task_1():
    spark = get_spark_session("Task_1_Ukrainian_Titles")

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
    akas_df = read_data(spark, 'title.akas.tsv.gz', schema_akas)

    # Transform: фільтрація за регіоном 'UA'
    result_t1 = (akas_df
        .filter(F.col('region') == 'UA')
        .select(F.col('title').alias('title_in_Ukrainian'))
        .distinct())

    # Load & Show
    result_t1.show(5, truncate=False)
    write_data(result_t1, 'task1_ukrainian_titles')

    spark.stop()

if __name__ == "__main__":
    run_task_1()