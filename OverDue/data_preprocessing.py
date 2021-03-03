# -*- coding: utf-8 -*-
import pandas as pd

# 读取数据
df = pd.read_csv("./data/cs-training.csv")

# 去除重复指
df.duplicated()
df.drop_duplicates()

# 查看各字段缺失率
print(dir(df))
# df.info()
# 缺失指按均值填充
for col in list(df.columns[df.isnull().sum() > 0]):
    mean_val = df[col].mean()
    df[col].fillna(mean_val, inplace=True)

# 删除不分析的列
columns = ["RevolvingUtilizationOfUnsecuredLines","DebtRatio","NumberOfOpenCreditLinesAndLoans","NumberOfTimes90DaysLate"]
df.drop(columns,axis=1,inplace=True)

# 保存至本地
# df.to_csv("./data/data.csv")