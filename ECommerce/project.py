# -*- coding: utf-8 -*
import json
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructField, StructType


sc = SparkContext('local', 'spark_project')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('com.databricks.spark.csv').options(header='true',interschema='true').load(
    'hdfs:///user/hadoop/pyspark/ECommerce/E_Commerce_Data_Clean.csv')
df.createOrReplaceTempView("data")
# df.show(2)

def save(path, data):
    with open(path, 'w') as f:
        f.write(data)

# (1) 客户数最多的10个国家
def countryCustomer():
    countryCustomerDF = spark.sql(
        "SELECT Country,COUNT(DISTINCT CustomerID) AS countOfCustomer FROM data GROUP BY Country ORDER BY countOfCustomer DESC LIMIT 10"
        )
    return countryCustomerDF.collect()

# (2) 销量最高的10个国家
def countryQuantity():
    countryQuantityDF = spark.sql(
        "SELECT Country,SUM(Quantity) AS sumOfQuantity FROM data GROUP BY Country ORDER BY sumOfQuantity DESC LIMIT 10"
        )
    return countryQuantityDF.collect()

# (3) 各个国家的总销售额分布情况
def countrySumOfPrice():
    countrySumOfPriceDF = spark.sql("SELECT Country,SUM(UnitPrice*Quantity) AS sumOfPrice FROM data GROUP BY Country")
    return countrySumOfPriceDF.collect()

# (4) 销量最高的10个商品
def stockQuantity():
    stockQuantityDF = spark.sql(
        "SELECT StockCode,SUM(Quantity) AS sumOfQuantity FROM data GROUP BY StockCode ORDER BY sumOfQuantity DESC LIMIT 10"
        )
    return stockQuantityDF.collect()

# (5) 商品描述的热门关键词Top300
def wordCount():
    wordCount = spark.sql("SELECT LOWER(Description) as description from data").rdd.flatMap(lambda line:line['description'].split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b).repartition(1).sortBy(lambda x:x[1],False)
 
    wordCountSchema = StructType([StructField("word", StringType(), True),StructField("count", IntegerType(), True)])
    wordCountDF = spark.createDataFrame(wordCount, wordCountSchema)
    wordCountDF = wordCountDF.filter(wordCountDF["word"]!='')
    return wordCountDF.take(300)

# (6) 退货订单数最多的10个国家
def countryReturnInvoice():
    countryReturnInvoiceDF = spark.sql(
        "SELECT Country,COUNT(DISTINCT InvoiceNo) AS countOfReturnInvoice FROM data WHERE InvoiceNo LIKE 'C%' GROUP BY Country ORDER BY countOfReturnInvoice DESC LIMIT 10"
        )
    return countryReturnInvoiceDF.collect()

# (7) 月销售额随时间的变化趋势
def formatData():
    tradeRDD = df.select("InvoiceDate","Quantity","UnitPrice",).rdd
    result1 = tradeRDD.map(lambda line: (line['InvoiceDate'].split(" ")[0], line['Quantity'] , line['UnitPrice']))
    result2 = result1.map(lambda line: (line[0].split("/"), line[1], line[2]))
    result3 = result2.map(lambda line: (line[0][2], line[0][0] if len(line[0][0])==2 else "0"+line[0][0], line[0][1] if len(line[0][1])==2 else "0"+line[0][1], line[1], line[2]))
    return result3

def tradePrice():
    result3 = formatData()
    result4 = result3.map(lambda line:(line[0]+"-"+line[1],int(line[3])*float(line[4])))
    result5 = result4.reduceByKey(lambda a,b:a+b).sortByKey()
    schema = StructType([StructField("date", StringType(), True),StructField("tradePrice", DoubleType(), True)])
    tradePriceDF = spark.createDataFrame(result5, schema)
    return tradePriceDF.collect()

# (8) 日销量随时间的变化趋势
def saleQuantity():
    result3 = formatData()
    result4 = result3.map(lambda line:(line[0]+"-"+line[1]+"-"+line[2],int(line[3])))
    result5 = result4.reduceByKey(lambda a,b:a+b).sortByKey()
    schema = StructType([StructField("date", StringType(), True),StructField("saleQuantity", IntegerType(), True)])
    saleQuantityDF = spark.createDataFrame(result5, schema)
    return saleQuantityDF.collect()

# (9) 各国的购买订单量和退货订单量的关系
def buyReturn():
    returnDF = spark.sql("SELECT Country AS Country,COUNT(DISTINCT InvoiceNo) AS countOfReturn FROM data WHERE InvoiceNo LIKE 'C%' GROUP BY Country")
    buyDF = spark.sql("SELECT Country AS Country2,COUNT(DISTINCT InvoiceNo) AS countOfBuy FROM data WHERE InvoiceNo NOT LIKE 'C%' GROUP BY Country2")
    buyReturnDF = returnDF.join(buyDF, returnDF["Country"] == buyDF["Country2"], "left_outer")
    buyReturnDF = buyReturnDF.select(buyReturnDF["Country"],buyReturnDF["countOfBuy"],buyReturnDF["countOfReturn"])
    return buyReturnDF.collect()

# (10) 商品的平均单价与销量的关系
def unitPriceSales():
    unitPriceSalesDF = spark.sql(
        "SELECT StockCode,AVG(DISTINCT UnitPrice) AS avgUnitPrice,SUM(Quantity) AS sumOfQuantity FROM data GROUP BY StockCode"
        )
    return unitPriceSalesDF.collect()


if __name__ == "__main__":

    base = "static/"
    if not os.path.exists(base):
        os.mkdir(base)

    m = {
        "countryCustomer": {
            "method": countryCustomer,
            "path": "countryCustomer.json"
        },
        "countryQuantity": {
            "method": countryQuantity,
            "path": "countryQuantity.json"
        },
        "countrySumOfPrice": {
            "method": countrySumOfPrice,
            "path": "countrySumOfPrice.json"
        },
        "stockQuantity": {
            "method": stockQuantity,
            "path": "stockQuantity.json"
        },
        "wordCount": {
            "method": wordCount,
            "path": "wordCount.json"
        },
        "countryReturnInvoice": {
            "method": countryReturnInvoice,
            "path": "countryReturnInvoice.json"
        },
        "tradePrice": {
            "method": tradePrice,
            "path": "tradePrice.json"
        },
        "saleQuantity": {
            "method": saleQuantity,
            "path": "saleQuantity.json"
        },
        "buyReturn": {
            "method": buyReturn,
            "path": "buyReturn.json"
        },
        "unitPriceSales": {
            "method": unitPriceSales,
            "path": "unitPriceSales.json"
        }
    }

    for k in m:
        p = m[k]
        f = p["method"]
        save(base + m[k]["path"], json.dumps(f()))
        print("done -> " + k + " , save to -> " + base + m[k]["path"])