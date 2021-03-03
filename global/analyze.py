# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
 
import pandas as pd
import matplotlib.pyplot as plt
import mpl_toolkits.basemap
 
conf = SparkConf().setMaster("local").setAppName("analyze")
sc = SparkContext(conf = conf)
sc.setLogLevel('WARN') # 减少不必要的log输出
spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()
 
dataFile = 'earthquakeData.csv'
 
rawData = spark.read.format('csv') \
                    .options(header='true', inferschema='true') \
                    .load(dataFile) 

# 1.全球每年发生重大地震的次数
def earthquakesPerYear():
    yearDF = rawData.select('Year').dropna().groupBy(rawData['Year']).count()
    yearDF = yearDF.sort(yearDF['Year'].asc())
    yearPd = yearDF.toPandas()
    # 数据可视化
    plt.bar(yearPd['Year'], yearPd['count'], color='skyblue')
    plt.title('earthquakes per year')
    plt.xlabel('Year')
    plt.ylabel('count')
    plt.show()
 
# 2.全球不同年份每月发生重大地震的次数
def earthquakesPerMonth():
    data = rawData.select('Year', 'Month').dropna()
    yearDF = data.groupBy(['Year', 'Month']).count()
    yearPd = yearDF.sort(yearDF['Year'].asc(), yearDF['Month'].asc()).toPandas()
    # 数据可视化
    x = [m+1 for m in range(12)]
    for groupName, group in yearPd.groupby('Year'):
        # plt.plot(x, group['count'], label=groupName)
        plt.scatter(x, group['count'], color='g', alpha=0.15)
    plt.title('earthquakes per month')
    # plt.legend() # 显示label
    plt.xlabel('Month')
    plt.show()
 
# 3.全球重大地震深度和强度之间的关系
def depthVsMagnitude():
    data = rawData.select('Depth', 'Magnitude').dropna()
    vsPd = data.toPandas()
    # 数据可视化
    plt.scatter(vsPd.Depth, vsPd.Magnitude, color='g', alpha=0.2)
    plt.title('Depth vs Magnitude')
    plt.xlabel('Depth')
    plt.ylabel('Magnitude')
    plt.show()
 
# 4.全球重大地震深度和类型之间的关系
def magnitudeVsType():
    data = rawData.select('Magnitude', 'Magnitude Type').dropna()
    typePd = data.toPandas()
    # 准备箱体数据
    typeBox = []
    typeName = []
    for groupName, group in typePd.groupby('Magnitude Type'):
        typeName.append(groupName)
        typeBox.append(group['Magnitude'])
    # 数据可视化
    plt.boxplot(typeBox, labels=typeName)
    plt.title('Type vs Magnitude')
    plt.xlabel('Type')
    plt.ylabel('Magnitude')
    plt.show()
 
# 5.全球经常发生重大地震的地带
def location():
    data = rawData.select('Latitude', 'Longitude', 'Magnitude').dropna()
    locationPd = data.toPandas()
    # 世界地图
    basemap = mpl_toolkits.basemap.Basemap()
    basemap.drawcoastlines()
    # 数据可视化
    plt.scatter(locationPd.Longitude, locationPd.Latitude,
                color='g', alpha=0.25, s=locationPd.Magnitude)
    plt.title('Location')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.show()
 
if __name__ == '__main__':
    earthquakesPerYear()
    # earthquakesPerMonth()
    # depthVsMagnitude()
    # magnitudeVsType()
    # location()