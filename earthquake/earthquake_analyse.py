# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

df = spark.read.csv("/user/hadoop/pyspark/earthquake/earthquake_cleaned.csv", 
                    header=True, 
                    inferSchema=True)
df = df.repartition(1)
# df.show(10)

df = df.withColumn("newDate", split(df['Date'], " ")[0]).drop("Date")
df = df.withColumnRenamed("newDate", "Date")
# df.show(10)

# 1.将数据按年份、月份、日期计数
attrsName = ['Year', 'Month', 'Day']
for i in range(len(attrsName)):
    df = df.withColumn(attrsName[i], split(df['Date'], "-")[i])
 
df.printSchema()

for x in attrsName:
    df = df.withColumn(x, df[x].cast('int'))    # 将年月日信息由字符串类型转换为整型

countByYear = df.groupBy("Year").count().orderBy("Year")
countByYear.toPandas().to_csv("countByYear.csv", 
                              encoding='utf-8', 
                              index=False)
 
countByMonth = df.groupBy("Month").count().orderBy("Month")
countByMonth.toPandas().to_csv("countByMonth.csv", 
                               encoding='utf-8', 
                               index=False)
 
countByDay = df.groupBy("Day").count().orderBy("Day")
countByDay.toPandas().to_csv("countByDay.csv", 
                             encoding='utf-8', 
                             index=False)

# 2.中国境内每个省份（海域）发生重大地震的次数
earthquakeC = df.filter("Area is not null")
earthquakeC.toPandas().to_csv("earthquakeC.csv", 
                              encoding='utf-8', 
                              index=False)

countByArea = earthquakeC.groupBy("Area").count()
countByArea.toPandas().to_csv("countByArea.csv", 
                              encoding='utf-8', 
                              index=False)

# 3.不同类型地震的数量
countByTypeC = earthquakeC.groupBy("Type").count()
countByTypeC.toPandas().to_csv("countByTypeC.csv", 
                               encoding='utf-8', 
                               index=False)

# 4.震级前500的地震
mostPow = df.sort(df["Magnitude"].desc(), df["Year"].desc()).take(500)
mostPowDF = spark.createDataFrame(mostPow)
mostPowDF.toPandas().to_csv("mostPow.csv", 
                            encoding='utf-8', 
                            index=False)

# 5.震源深度前500的地震
mostDeep = df.sort(df["Depth"].desc(), df["Magnitude"].desc()).take(500)
mostDeepDF = spark.createDataFrame(mostDeep)
mostDeepDF.toPandas().to_csv("mostDeep.csv", 
                             encoding='utf-8', 
                             index=False)

# 6.震级与震源深度的关系
df.select(df["Magnitude"], df["Depth"]).toPandas().to_csv("powDeep.csv", 
                                                          encoding='utf-8', 
                                                          index=False)

df.toPandas().to_csv("earthquake1.csv", 
                     encoding='utf-8', 
                     index=False)