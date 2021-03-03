# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions
 
def analyse(filename):
    # 读取数据
    spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()
    df = spark.read.format("csv").option("header","true").load(filename)
 
    # 修改列名
    df = df.withColumnRenamed('SeriousDlqin2yrs','y')
    df = df.withColumnRenamed('NumberOfTime30-59DaysPastDueNotWorse','30-59days')
    df = df.withColumnRenamed('NumberOfTime60-89DaysPastDueNotWorse','60-89days')
    df = df.withColumnRenamed('NumberRealEstateLoansOrLines','RealEstateLoans')
    df = df.withColumnRenamed('NumberOfDependents','families')
 
    # 返回data_web.py的数据列表
    all_list = []
    # 本次信用卡逾期分析
    # 共有逾期10026人，139974没有逾期，总人数150000
    total_y = []
    for i in range(2):
        total_y.append(df.filter(df['y'] == i).count())
    all_list.append(total_y)
 
    # 年龄分析
    df_age = df.select(df['age'],df['y'])
    agenum = []
    bin = [0,30,45,60,75,100]
    # 统计各个年龄段的人口
    for i in range(5):
        agenum.append(df_age.filter(df['age'].between(bin[i],bin[i+1])).count())
    all_list.append(agenum)
    # 统计各个年龄段逾期与不逾期的数量
    age_y = []
    for i in range(5):
        y0 = df_age.filter(df['age'].between(bin[i],bin[i+1])).\
            filter(df['y']=='0').count()
        y1 = df_age.filter(df['age'].between(bin[i],bin[i+1])).\
            filter(df['y']=='1').count()
        age_y.append([y0,y1])
    all_list.append(age_y)
 
    # 有逾期记录的人的本次信用卡逾期数量
    df_pastDue = df.select(df['30-59days'],df['60-89days'],df['y'])
    # 30-59有23982人，4985逾期，18997不逾期
    numofpastdue = []
    numofpastdue.append(df_pastDue.filter(df_pastDue['30-59days'] > 0).count())
    y_numofpast1 = []
    for i in range(2):
        x = df_pastDue.filter(df_pastDue['30-59days'] > 0).\
            filter(df_pastDue['y'] == i).count()
        y_numofpast1.append(x)
    # 60-89有7604人，2770逾期，4834不逾期
    numofpastdue.append(df_pastDue.filter(df_pastDue['60-89days'] > 0).count())
    y_numofpast2 = []
    for i in range(2):
        x = df_pastDue.filter(df_pastDue['60-89days'] > 0).\
            filter(df_pastDue['y'] == i).count()
        y_numofpast2.append(x)
    # 两个记录都有的人有4393人，逾期1907，不逾期2486
    numofpastdue.append(df_pastDue.filter(df_pastDue['30-59days'] > 0).
                        filter(df_pastDue['60-89days'] > 0).count())
    y_numofpast3 = []
    for i in range(2):
        x = df_pastDue.filter(df_pastDue['30-59days'] > 0).\
            filter(df_pastDue['60-89days'] > 0).filter(df_pastDue['y'] == i).count()
        y_numofpast3.append(x)
    all_list.append(numofpastdue)
    all_list.append(y_numofpast1)
    all_list.append(y_numofpast2)
    all_list.append(y_numofpast3)
 
    # 房产抵押数量分析
    df_Loans = df.select(df['RealEstateLoans'],df['y'])
    # 有无抵押房产人数情况
    numofrealandnoreal = []
    numofrealandnoreal.append(df_Loans.filter(df_Loans['RealEstateLoans']==0).count())
    numofrealandnoreal.append(df_Loans.filter(df_Loans['RealEstateLoans']>0).count())
    all_list.append(numofrealandnoreal)
    ## 房产无抵押共有56188人，逾期4672人，没逾期51516人
    norealnum = []
    for i in range(2):
        x = df_Loans.filter(df_Loans['RealEstateLoans']==0).\
            filter(df_Loans['y'] == i).count()
        norealnum.append(x)
    all_list.append(norealnum)
    # 房产抵押共有93812人，逾期5354人，不逾期88458人
    realnum = []
    for i in range(2):
        x = df_Loans.filter(df_Loans['RealEstateLoans']>0).\
            filter(df_Loans['y'] == i).count()
        realnum.append(x)
    all_list.append(realnum)
 
    # 家属人数分析
    df_families = df.select(df['families'],df['y'])
    # 有无家属人数统计
    nofamiliesAndfamilies = []
    nofamiliesAndfamilies.append(df_families.filter(df_families['families']>0).count())
    nofamiliesAndfamilies.append(df_families.filter(df_families['families']==0).count())
    all_list.append(nofamiliesAndfamilies)
    # 有家属59174人，逾期4752人，没逾期54422人
    y_families = []
    y_families.append(df_families.filter(df_families['families']>0).
                      filter(df_families['y']==0).count())
    y_families.append(df_families.filter(df_families['families']>0).
                      filter(df_families['y']==1).count())
    all_list.append(y_families)
    # 没家属90826人，逾期5274人，没逾期85552人
    y_nofamilies = []
    y_nofamilies.append(df_families.filter(df_families['families']==0).
                        filter(df_families['y']==0).count())
    y_nofamilies.append(df_families.filter(df_families['families']==0).
                        filter(df_families['y']==1).count())
    all_list.append(y_nofamilies)
 
    # 月收入分析
    df_income = df.select(df['MonthlyIncome'],df['y'])
    # 获取平均值，其中先返回Row对象，再获取其中均值
    mean_income = df_income.agg(functions.avg(df_income['MonthlyIncome'])).head()[0]
    # 收入分布，105854人没超过均值6670，44146人超过均值6670
    numofMeanincome = []
    numofMeanincome.append(df_income.filter(df['MonthlyIncome'] < mean_income).count())
    numofMeanincome.append(df_income.filter(df['MonthlyIncome'] > mean_income).count())
    all_list.append(numofMeanincome)
    # 未超过均值的逾期情况分析,97977人没逾期，7877人逾期
    y_NoMeanIncome = []
    y_NoMeanIncome.append(df_income.filter(df['MonthlyIncome'] < mean_income).filter(df['y']==0).count())
    y_NoMeanIncome.append(df_income.filter(df['MonthlyIncome'] < mean_income).filter(df['y']==1).count())
    all_list.append(y_NoMeanIncome)
    # 超过均值的逾期情况分析，41997人没逾期，2149人逾期
    y_MeanIncome = []
    y_MeanIncome.append(df_income.filter(df['MonthlyIncome'] > mean_income).filter(df['y']==0).count())
    y_MeanIncome.append(df_income.filter(df['MonthlyIncome'] > mean_income).filter(df['y']==1).count())
    all_list.append(y_MeanIncome)
 
    # 数据可视化data_web.py
    return all_list