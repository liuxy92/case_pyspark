# -*- coding: utf-8 -*-
import json
import os
import pandas as pd
import matplotlib.pyplot as plt
 
 
AVE_REVIEW_CATEGORY = '/usr/local/spark/mycode/yelp/analysis/average_review_category'
# OPEN_CLOSE = '/usr/local/spark/mycode/yelp/analysis/open_close'
TOP_CATEGORY_CITY = '/usr/local/spark/mycode/yelp/analysis/top_category_city'
TOP_BUSINESS_CITY = '/usr/local/spark/mycode/yelp/analysis/top_business_city'
TOP_CATEGORY = '/usr/local/spark/mycode/yelp/analysis/top_category'
AVE_STARS_CATEGORY = '/usr/local/spark/mycode/yelp/analysis/average_stars_category'
TAKEOUT = '/usr/local/spark/mycode/yelp/analysis/RestaurantsTakeout'
 
def read_json(file_path):
    json_path_names = os.listdir(file_path)
    data = []
    for idx in range(len(json_path_names)):
        json_path = file_path + '/' + json_path_names[idx]
        if json_path.endswith('.json'):
            with open(json_path) as f:
                for line in f:
                    data.append(json.loads(line))
    return data
 
 
 
if __name__ == '__main__':
    ave_review_category_list = read_json(AVE_REVIEW_CATEGORY)
    # open_close_list = read_json(OPEN_CLOSE)
    top_category_city_list = read_json(TOP_CATEGORY_CITY)
    top_business_city_list = read_json(TOP_BUSINESS_CITY)
    top_category_list = read_json(TOP_CATEGORY)
    ave_stars_category_list = read_json(AVE_STARS_CATEGORY)
    takeout_list = read_json(TAKEOUT)
 
 
    top_category_list.sort(key=lambda x: x['freq'], reverse=True)
    top_category_key = []
    top_category_value = []
    for idx in range(10):
        one = top_category_list[idx]
        top_category_key.append(one['new_category'])
        top_category_value.append(one['freq'])
 
    plt.barh(top_category_key[:10], top_category_value[:10], tick_label=top_category_key[:10])
    plt.title('Top 10 Categories', size = 16)
    plt.xlabel('Frequency',size =8, color = 'Black')
    plt.ylabel('Category',size = 8, color = 'Black')
    plt.tight_layout()
 
 
    top_business_city_list.sort(key=lambda x: x['no_of_bus'], reverse=True)
    top_business_city_key = []
    top_business_city_value = []
    for idx in range(10):
        one = top_business_city_list[idx]
        top_business_city_key.append(one['no_of_bus'])
        top_business_city_value.append(one['city'])
 
    """
    plt.barh(top_business_city_value[:10], top_business_city_key[:10], tick_label=top_business_city_value[:10])
    plt.title('Top 10 Cities with most businesses', size = 16)
    plt.xlabel('no_of_number',size =8, color = 'Black')
    plt.ylabel('city',size = 8, color = 'Black')
    plt.tight_layout()
    """
 
    ave_review_category_list.sort(key=lambda x: x['avg_review_count'], reverse=True)
    ave_review_category_key = []
    ave_review_category_value = []
    for idx in range(10):
        one = ave_review_category_list[idx]
        ave_review_category_key.append(one['avg_review_count'])
        ave_review_category_value.append(one['new_category'])
 
    """
    plt.barh(ave_review_category_value[:10], ave_review_category_key[:10], tick_label=ave_review_category_value[:10])
    plt.title('Top 10 categories with most review', size=16)
    plt.xlabel('avg_review_count', size=8, color='Black')
    plt.ylabel('category', size=8, color='Black')
    plt.tight_layout()
    """
 
 
    ave_stars_category_list.sort(key=lambda x: x['avg_stars'], reverse=True)
    ave_stars_category_key = []
    ave_stars_category_value = []
    for idx in range(10):
        one = ave_stars_category_list[idx]
        ave_stars_category_key.append(one['avg_stars'])
        ave_stars_category_value.append(one['new_category'])
 
    """
    plt.barh(ave_stars_category_value[:10], ave_stars_category_key[:10], tick_label=ave_stars_category_value[:10])
    plt.title('Top 10 categories with most stars', size=16)
    plt.xlabel('avg_stars', size=8, color='Black')
    plt.ylabel('category', size=8, color='Black')
    plt.tight_layout()
    """
    takeout_list.sort(key=lambda x: x['stars'], reverse=True)
    takeout_key = []
    takeout_value = []
    for idx in range(len(takeout_list)):
        one = takeout_list[idx]
        takeout_key.append(one['stars'])
        takeout_value.append(one['RestaurantsTakeout'])
    """
    explode = (0,0,0)
    plt.pie(takeout_key,explode=explode,labels=takeout_value, autopct='%1.1f%%',shadow=False, startangle=150)
    plt.title('Whether take out or not', size=16)
    plt.axis('equal')
    plt.tight_layout()
    """
    plt.show()