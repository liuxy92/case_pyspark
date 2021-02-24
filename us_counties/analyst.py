# -*- coding: utf-8 -*-
import os
os.environ["PYSPARK_PYTHON"]="/home/hadoop/anaconda2/envs/py3/bin/python"

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime

import pyspark.sql.functions as func

def toDate(inputStr):
    newStr = ""
    if len(inputStr) == 8:
        s1 = inputStr[0:4]
        s2 = inputStr[5:6]
        s3 = inputStr[7]
        newStr = s1+"-"+"0"+s2+"-"+"0"+s3
    else:
        s1 = inputStr[0:4]
        s2 = inputStr[5:6]
        s3 = inputStr[7:]
        newStr = s1+"-"+"0"+s2+"-"+s3
    date = datetime.strptime(newStr, "%Y-%m-%d")
    return date
 
 
 
#主程序:
spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()
 
fields = [StructField("date", DateType(),False),StructField("county", StringType(),False),StructField("state", StringType(),False),
                    StructField("cases", IntegerType(),False),StructField("deaths", IntegerType(),False),]
schema = StructType(fields)
 
rdd0 = spark.sparkContext.textFile("/user/hadoop/us_counties/us-counties.txt")
rdd1 = rdd0.map(lambda x:x.split("\t")).map(lambda p: Row(toDate(p[0]),p[1],p[2],int(p[3]),int(p[4])))
 
 
shemaUsInfo = spark.createDataFrame(rdd1,schema)
 
shemaUsInfo.createOrReplaceTempView("usInfo")
 
#1.计算每日的累计确诊病例数和死亡数
df = shemaUsInfo.groupBy("date").agg(func.sum("cases"),func.sum("deaths")).sort(shemaUsInfo["date"].asc())
 
#列重命名
df1 = df.withColumnRenamed("sum(cases)","cases").withColumnRenamed("sum(deaths)","deaths")
df1.repartition(1).write.json("us_counties/result1.json")                               #写入hdfs
 
#注册为临时表供下一步使用
df1.createOrReplaceTempView("ustotal")
 
#2.计算每日较昨日的新增确诊病例数和死亡病例数
df2 = spark.sql("select t1.date,t1.cases-t2.cases as caseIncrease,t1.deaths-t2.deaths as deathIncrease from ustotal t1,ustotal t2 where t1.date = date_add(t2.date,1)")
 
df2.sort(df2["date"].asc()).repartition(1).write.json("us_counties/result2.json")           #写入hdfs
 
#3.统计截止5.19日 美国各州的累计确诊人数和死亡人数
df3 = spark.sql("select date,state,sum(cases) as totalCases,sum(deaths) as totalDeaths,round(sum(deaths)/sum(cases),4) as deathRate from usInfo  where date = to_date('2020-05-19','yyyy-MM-dd') group by date,state")
 
df3.sort(df3["totalCases"].desc()).repartition(1).write.json("us_counties/result3.json") #写入hdfs
 
df3.createOrReplaceTempView("eachStateInfo")
 
#4.找出美国确诊最多的10个州
df4 = spark.sql("select date,state,totalCases from eachStateInfo  order by totalCases desc limit 10")
df4.repartition(1).write.json("us_counties/result4.json")
 
#5.找出美国死亡最多的10个州
df5 = spark.sql("select date,state,totalDeaths from eachStateInfo  order by totalDeaths desc limit 10")
df5.repartition(1).write.json("us_counties/result5.json")
 
#6.找出美国确诊最少的10个州
df6 = spark.sql("select date,state,totalCases from eachStateInfo  order by totalCases asc limit 10")
df6.repartition(1).write.json("us_counties/result6.json")
 
#7.找出美国死亡最少的10个州
df7 = spark.sql("select date,state,totalDeaths from eachStateInfo  order by totalDeaths asc limit 10")
df7.repartition(1).write.json("us_counties/result7.json")
 
#8.统计截止5.19全美和各州的病死率
df8 = spark.sql("select 1 as sign,date,'USA' as state,round(sum(totalDeaths)/sum(totalCases),4) as deathRate from eachStateInfo group by date union select 2 as sign,date,state,deathRate from eachStateInfo").cache()
df8.sort(df8["sign"].asc(),df8["deathRate"].desc()).repartition(1).write.json("us_counties/result8.json")