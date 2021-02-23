# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
 
def data_process(raw_data_path):
 
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(raw_data_path)
    split_col = f.split(business['categories'], ',')
    business = business.withColumn("categories", split_col).filter(business["city"] != "").dropna()
    business.createOrReplaceTempView("business")
 
    b_etl = spark.sql("SELECT business_id, name, city, state, latitude, longitude, stars, review_count, is_open, categories, attributes FROM business").cache()
    b_etl.createOrReplaceTempView("b_etl")
    outlier = spark.sql(
        "SELECT b1.business_id, SQRT(POWER(b1.latitude - b2.avg_lat, 2) + POWER(b1.longitude - b2.avg_long, 2)) \
        as dist FROM b_etl b1 INNER JOIN (SELECT state, AVG(latitude) as avg_lat, AVG(longitude) as avg_long \
        FROM b_etl GROUP BY state) b2 ON b1.state = b2.state ORDER BY dist DESC")
    outlier.createOrReplaceTempView("outlier")
    joined = spark.sql("SELECT b.* FROM b_etl b INNER JOIN outlier o ON b.business_id = o.business_id WHERE o.dist<10")
    joined.write.parquet("file:///home/hadoop/mycode/case_pyspark/yelp/data/yelp-etl/business_etl", mode="overwrite")
 
 
if __name__ == "__main__":
    raw_hdfs_path = 'file:///home/hadoop/mycode/case_pyspark/yelp/data/yelp_academic_dataset_business.json'
    print("Start cleaning raw data!")
    data_process(raw_hdfs_path)
    print("Successfully done")