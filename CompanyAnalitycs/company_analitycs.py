import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import re
spark = SparkSession.builder.getOrCreate()


def open_data_from_opros():
    list_names = []
    for i in range(11):
        try:
            with open(f'opros\\result_{i}.txt', 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.replace('\n', '')
                    find_str = re.match(r'[А-Я][а-я]+\s[А-Я,ё][а-я]+', line)

                    if find_str != None:
                        list_names.append({
                            'fi':line[line.find(' ')+1:len(line)]\
                                          + ' ' + line[0:line.find(' ')],
                            'emotion':i
                        })
        except:
            pass
    return list_names


df = spark.read.option("header", True).csv("all_groups.csv")

list_data = open_data_from_opros()

dataCollect = df.select("fio").distinct().collect()
for row in dataCollect:
    for count in range(len(list_data)):
        str_find: str = fr'{str((dict(list_data[count]))["fi"])}\s[А-Я, Ё][а-я, ё]+'
        finder = re.match(str_find, str(row['fio']))
        if finder is not None:
            list_data[count]["fi"] = str(row['fio'])


df_emotion = spark.createDataFrame(list_data).orderBy("fi")


all_data_people_with_emotion = df.join(df_emotion, df.fio == df_emotion.fi, "left_outer")

# тут данные о том, кто отреагировал на опрос + все данные о людях
full_result = all_data_people_with_emotion.select("fio", "priority", "company", "work",
                                    "go_to_company", "phone", "email", "group", "emotion")

# итоговый рейтинг компаний
print("итоговый рейтинг компаний")
result_go_to_company = full_result\
    .filter(F.col("go_to_company") == 1)\
    .filter(F.col("emotion").isNotNull())\
    .orderBy(F.col("fio")).select("company", "emotion")

result_go_to_company.groupBy("company")\
    .agg(F.avg("emotion").alias("result_emotion"),
         F.min("emotion").alias("min_emotion"))\
    .sort(F.avg("emotion").desc()).show()
    # \
    # .write.option("header", True).csv("results\\итоговый_рейтинг.csv")

print("в опросе проголосовали")
result_go_to_company.groupBy("company")\
    .count().sort(F.count("emotion").desc()).write.option("header", True).csv("results\\число_голосовавших.csv")


print("загруженность заявок в компаниях по 2-м группам (до практики)")
full_result.filter(F.col("priority")==1)\
    .select("company").groupBy("company").count().sort(F.count("company").desc())\
    .write.option("header", True).csv("results\\загруженность_по2_группам.csv")


print("наиболее приоритетная компания до практики у биц-201 (до практики)")
full_result.filter(F.col("priority")==1).filter(F.col("group") == "бИСТ-201")\
    .select("company").groupBy("company").count().sort(F.count("company").desc())\
    .write.option("header", True).csv("results\\загруженность_по_биц.csv")

print("наиболее приоритетная компаня до практики у брис-201 (до практики)")
full_result.filter(F.col("priority")==1).filter(F.col("group") == "бРИС-201")\
    .select("company").groupBy("company").count().sort(F.count("company").desc())\
    .write.option("header", True).csv("results\\загруженность_по_брис.csv")

print("количество людей, чьи запросы не были удовлетворены до начала практики по компаниям")
full_result.filter(F.col("priority")==1).filter(F.col("go_to_company")!=1)\
    .groupBy("company").count()\
    .write.option("header", True).csv("results\\промахнулись.csv")


