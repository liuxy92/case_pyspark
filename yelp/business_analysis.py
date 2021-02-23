# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os
 
def attribute_score(spark, attribute):
    att = spark.sql("SELECT attributes.{attr} as {attr}, category, stars FROM for_att".format(attr=attribute)).dropna()
    att.createOrReplaceTempView("att")
    att_group = spark.sql("SELECT {attr}, AVG(stars) AS stars FROM att GROUP BY {attr} ORDER BY stars".format(attr=attribute))
    att_group.show()    
    att_group.write.json("file:///usr/local/spark/mycode/yelp/analysis/{attr}".format(attr=attribute), mode='overwrite')
 
 
 
def analysis(data_path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.parquet(data_path).cache()
    business.createOrReplaceTempView("business")
 
    part_business = spark.sql("SELECT state, city, stars, review_count, explode(categories) AS category FROM business").cache()
    part_business.show()
    part_business.createOrReplaceTempView('part_business_1')
    part_business = spark.sql("SELECT state, city, stars, review_count, REPLACE(category, ' ','')as new_category FROM part_business_1")
    part_business.createOrReplaceTempView('part_business')
 
 
    print("## All distinct categories")
    all_categories = spark.sql("SELECT business_id, explode(categories) AS category FROM business")
    all_categories.createOrReplaceTempView('all_categories')
 
    distinct = spark.sql("SELECT COUNT(DISTINCT(new_category)) FROM part_business")
    distinct.show()
 
    print("## Top 10 business categories")
    top_cat = spark.sql("SELECT new_category, COUNT(*) as freq FROM part_business GROUP BY new_category ORDER BY freq DESC")
    top_cat.show(10)   
    top_cat.write.json("file:///usr/local/spark/mycode/yelp/analysis/top_category", mode='overwrite')
 
    print("## Top business categories - in every city")
    top_cat_city = spark.sql("SELECT city, new_category, COUNT(*) as freq FROM part_business GROUP BY city, new_category ORDER BY freq DESC")
    top_cat_city.show()  
    top_cat.write.json("file:///usr/local/spark/mycode/yelp/analysis/top_category_city", mode='overwrite')
 
    print("## Cities with most businesses")
    bus_city = spark.sql("SELECT city, COUNT(business_id) as no_of_bus FROM business GROUP BY city ORDER BY no_of_bus DESC")
    bus_city.show(10)   
    bus_city.write.json("file:///usr/local/spark/mycode/yelp/analysis/top_business_city", mode='overwrite')
 
    print("## Average review count by category")
    avg_city = spark.sql(
        "SELECT new_category, AVG(review_count)as avg_review_count FROM part_business GROUP BY new_category ORDER BY avg_review_count DESC")
    avg_city.show()  
    avg_city.write.json("file:///usr/local/spark/mycode/yelp/analysis/average_review_category", mode='overwrite')
 
 
    print("## Average stars by category")
    avg_state = spark.sql(
        "SELECT new_category, AVG(stars) as avg_stars FROM part_business GROUP BY new_category ORDER BY avg_stars DESC")
    avg_state.show()   
    avg_state.write.json("file:///usr/local/spark/mycode/yelp/analysis/average_stars_category", mode='overwrite')
 
    print("## Data based on Attribute")
    for_att = spark.sql("SELECT attributes, stars, explode(categories) AS category FROM business")
    for_att.createOrReplaceTempView("for_att")
    attribute = 'RestaurantsTakeout'
    attribute_score(spark, attribute)
 
 
if __name__ == "__main__":
    business_data_path = 'file:///home/hadoop/mycode/case_pyspark/yelp/data/yelp-etl/business_etl' 
    print("Start analysis data!")
    analysis(business_data_path)
    print("Analysis done")