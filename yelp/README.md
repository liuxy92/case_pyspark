# 基于YELP数据集的商业数据分析

### 一、数据集
本次实验数使用的数据集是来自Kaggle的Yelp数据集。这里选择了其中的yelp_academic_dataset_business.json数据集。  
数据集下载链接为：https://www.kaggle.com/yelp-dataset/yelp-dataset  
或百度网盘地址1：https://pan.baidu.com/s/1FmUO1NWC0DTLZKG6ih6TYQ (提取码：mber)，  
或百度网盘地址2：https://pan.baidu.com/s/1I2MBR7nYDKFOLe2FW96zTQ （提取码：61im）  
数据集为json 格式，每个数据包含以下字段：

```
字段名称 含义 数据格式 例子

business_id 商家ID string “business_id”: “tnhfDv5Il8EaGSXZGiuQGg”

name 商家名称 string “name”: “Garaje”

address 商家地址 string “address”: “475 3rd St”

city 商家所在城市 string “city”: “San Francisco”

state 商家所在洲 string “state”: “CA”

postal code 邮编 string “postal code”: “94107”

latitude 维度 float “latitude”: 37.7817529521

longitude 经度 float “longitude”: -122.39612197

stars 星级评分 float “stars”: 4.5

review_count 评论个数 integer “review_count”: 1198

is_open 商家是否营业

0：关闭， 1：营业 integer “is_open”: 1

attributes 商家业务(外卖，business parking) object “attributes”: {
“RestaurantsTakeOut”: true,
“BusinessParking”: {
“garage”: false,
“street”: true,
“validated”: false,
“lot”: false,
“valet”: false
},
}

categories 商家所属类别 array “categories”: [
“Mexican”,
“Burgers”,
“Gastropubs”
]

hours 商家营业时间 dict “hours”: {
“Monday”: “10:00-21:00”,
“Tuesday”: “10:00-21:00”,
“Friday”: “10:00-21:00”,
“Wednesday”: “10:00-21:00”,
“Thursday”: “10:00-21:00”,
“Sunday”: “11:00-18:00”,
“Saturday”: “10:00-21:00”
}
```

### 二、步骤概述
1）第1步：使用代码文件business_process.py， 对数据进行预处理，剔除异常值。  
2）第2步：使用代码文件business_analysis.py， 对处理后的数据进行数据分析。  
3）第3步：使用代码文件business_visual.py， 对分析结果进行可视化。