from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, \
        StringType, IntegerType

spark = SparkSession.builder.getOrCreate()


dag = DAG(
    'my_easy_dag',
    start_date=days_ago(0, 0, 0, 0)
)


# functions--------------------------------


def create_df_people():
    data = [
        [0, "Ivanov", "Ivan", 20, "8763452312", 30000],
        [1, "Romashkin", "Petr", 51, "8763452313", 15000],
        [3, "Alekseev", "Mihail", 34, "8763452314", 60000],
        [4, "Alekseev", "Andrey", 34, "8763452315", 12000],
        [5, "Berezov", "Iogan", 39, "8763452317", 13564],
        [7, "Stepashkin", "Roman", 47, "8763452318", 54000]
    ]
    schema = StructType([
        StructField("Worker_id", IntegerType(), True),
        StructField("Lastname", StringType(), True),
        StructField("Firstname", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Phone", StringType(), True),
        StructField("Salary", IntegerType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df.show()
    df.coalesce(1).write.option("header", True).csv("/root/my_files/people.csv")


def show_download_df():
    df = spark.read.option("header", True).csv("/root/my_files/preferences.csv")
    df.show()


def join_dfs():
    df1 = spark.read.option("header", True).csv("/root/my_files/people.csv")
    df2 = spark.read.option("header", True).csv("/root/my_files/preferences.csv")
    result = df1.join(df2, df1.Worker_id==df2.Worker_id_, 'left_outer')
    result.show()
    result.coalesce(1).write.option("header", True).csv("/root/my_files/people_preferences.csv")

def statistic_animals():
    df = spark.read.option("header", True).csv("/root/my_files/people_preferences.csv")
    df = df.groupBy(F.col("Favorite_animals")).count()
    df.show()
    df.coalesce(1).write.option("header", True).csv("/root/my_files/animals_statistic.csv")


def statistic_workers_salary_age_last():
    df = spark.read.option("header", True).csv("/root/my_files/people_preferences.csv")
    df = df.filter(F.col("Age") > 30)
    df.show()
    df.coalesce(1).write.option("header", True).csv("/root/my_files/workers_statistic.csv")


# operations-------------------------------


operation_create_df_people = PythonOperator(
    dag=dag,
    task_id="create_df_people",
    python_callable=create_df_people
)

operation_show_download_df = PythonOperator(
    dag=dag,
    task_id="show_download_df",
    python_callable=show_download_df
)

operation_join_dfs = PythonOperator(
    dag=dag,
    task_id="join_dfs",
    python_callable=join_dfs
)

operation_df_statistics_animals = PythonOperator(
    dag=dag,
    task_id="statics_animals",
    python_callable=statistic_animals
)

operation_df_statistics_worker_more_30 = PythonOperator(
    dag=dag,
    task_id="statics_workers_more_30",
    python_callable=statistic_workers_salary_age_last
)


# start--------------------------------------


[operation_create_df_people, operation_show_download_df] >> operation_join_dfs
operation_join_dfs >> [operation_df_statistics_animals, operation_df_statistics_worker_more_30]
