import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *


class HandlerDF:
    __data = [
        ["Ivanov", "Ivan", 20, "8763452312"],
        ["Romashkin", "Petr", 51, "8763452313"],
        ["Alekseev", "Mihail", 34, "8763452314"],
        ["Alekseev", "Andrey", 34, "8763452315"],
        ["Berezov", "Iogan", 39, "8763452317"],
        ["Stepashkin", "Roman", 47, "8763452318"]
    ]
    __schema = StructType([
        StructField("Lastname", StringType(), True),
        StructField("Firstname", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Phone", StringType(), True)
    ])
    __df: pyspark.sql.DataFrame = None

    def __init__(self, spark: pyspark.sql.SparkSession):
        self.__df = spark.createDataFrame(self.__data, self.__schema)

    def print_df(self):
        self.__df.show()

    def save_parquet(self):
        self.__df.write.parquet("people.parquet")

    def processing_df(self):
        self.__df = self.__df.\
            filter(F.col("age") < 40).\
            orderBy("age", ascending=True)

    def save_csv(self):
        self.__df.coalesce(1).write.option("header", True).csv("people.csv", sep=";")

def main():
    spark = SparkSession.builder.getOrCreate()
    handler = HandlerDF(spark)

    handler.save_parquet()
    handler.print_df()

    handler.processing_df()
    handler.print_df()
    handler.save_csv()

if __name__ == "__main__":
    main()
