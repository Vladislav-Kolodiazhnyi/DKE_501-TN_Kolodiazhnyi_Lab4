import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import get_spark_session, read_data, write_data

def run_task_2():
    spark = get_spark_session("Task_2_19th_Century_Persons")

    # Схема для name.basics.tsv.gz
    schema_names = T.StructType([
        T.StructField('nconst', T.StringType(), False),
        T.StructField('primaryName', T.StringType(), True),
        T.StructField('birthYear', T.IntegerType(), True),
        T.StructField('deathYear', T.IntegerType(), True),
        T.StructField('primaryProfession', T.StringType(), True),
        T.StructField('knownForTitles', T.StringType(), True)
    ])

    # Extract
    names_df = read_data(spark, 'name.basics.tsv.gz', schema_names)

    # Transform: народжені у 1801-1900 роках
    result_t2 = (names_df
        .filter(F.col('birthYear').between(1801, 1900))
        .select(
            F.col('primaryName').alias('person_name'),
            F.col('birthYear').alias('birth_year')
        )
        .distinct())

    # Load & Show
    result_t2.show(5, truncate=False)
    write_data(result_t2, 'task2_born_in_19th_century')

    spark.stop()

if __name__ == "__main__":
    run_task_2()