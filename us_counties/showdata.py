# -*- coding: utf-8 -*-
import os
os.environ["PYSPARK_PYTHON"]="/home/hadoop/anaconda2/envs/py3/bin/python"

from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.charts import Line
from pyecharts.components import Table
from pyecharts.charts import WordCloud
from pyecharts.charts import Pie
from pyecharts.charts import Funnel
from pyecharts.charts import Scatter
from pyecharts.charts import PictorialBar
from pyecharts.options import ComponentTitleOpts
from pyecharts.globals import SymbolType
import json
 
 
 
#1.画出每日的累计确诊病例数和死亡数——>双柱状图
def drawChart_1(index):
    # root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    root = "/home/hadoop/mycode/case_pyspark/us_counties/data/result/result" + str(index) +"/part-00000-4b61e479-6ed2-4280-b176-a9aaf118b3c7-c000.json"
    date = []
    cases = []
    deaths = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            date.append(str(js['date']))
            cases.append(int(js['cases']))
            deaths.append(int(js['deaths']))
 
    d = (
    Bar()
    .add_xaxis(date)
    .add_yaxis("累计确诊人数", cases, stack="stack1")
    .add_yaxis("累计死亡人数", deaths, stack="stack1")
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(title_opts=opts.TitleOpts(title="美国每日累计确诊和死亡人数"))
    .render("/home/hadoop/mycode/case_pyspark/us_counties/data/result/result1/result1.html")
    )
 

'''
#2.画出每日的新增确诊病例数和死亡数——>折线图
def drawChart_2(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    date = []
    cases = []
    deaths = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            date.append(str(js['date']))
            cases.append(int(js['caseIncrease']))
            deaths.append(int(js['deathIncrease']))
 
    (
    Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
    .add_xaxis(xaxis_data=date)
    .add_yaxis(
        series_name="新增确诊",
        y_axis=cases,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="最大值")
 
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[opts.MarkLineItem(type_="average", name="平均值")]
        ),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="美国每日新增确诊折线图", subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(is_show=True),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
    )
    .render("/home/hadoop/result/result2/result1.html")
    )
    (
    Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
    .add_xaxis(xaxis_data=date)
    .add_yaxis(
        series_name="新增死亡",
        y_axis=deaths,
        markpoint_opts=opts.MarkPointOpts(
            data=[opts.MarkPointItem(type_="max", name="最大值")]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="average", name="平均值"),
                opts.MarkLineItem(symbol="none", x="90%", y="max"),
                opts.MarkLineItem(symbol="circle", type_="max", name="最高点"),
            ]
        ),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="美国每日新增死亡折线图", subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(is_show=True),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
    )
    .render("/home/hadoop/result/result2/result2.html")
    )
 
 
 
 
#3.画出截止5.19，美国各州累计确诊、死亡人数和病死率--->表格
def drawChart_3(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    allState = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            row = []
            row.append(str(js['state']))
            row.append(int(js['totalCases']))
            row.append(int(js['totalDeaths']))
            row.append(float(js['deathRate']))
            allState.append(row)
 
    table = Table()
 
    headers = ["State name", "Total cases", "Total deaths", "Death rate"]
    rows = allState
    table.add(headers, rows)
    table.set_global_opts(
        title_opts=ComponentTitleOpts(title="美国各州疫情一览", subtitle="")
    )
    table.render("/home/hadoop/result/result3/result1.html")
 
 
#4.画出美国确诊最多的10个州——>词云图
def drawChart_4(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            row=(str(js['state']),int(js['totalCases']))
            data.append(row)
 
    c = (
    WordCloud()
    .add("", data, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="美国各州确诊Top10"))
    .render("/home/hadoop/result/result4/result1.html")
    )
 
 
 
 
#5.画出美国死亡最多的10个州——>象柱状图
def drawChart_5(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    state = []
    totalDeath = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            state.insert(0,str(js['state']))
            totalDeath.insert(0,int(js['totalDeaths']))
 
    c = (
    PictorialBar()
    .add_xaxis(state)
    .add_yaxis(
        "",
        totalDeath,
        label_opts=opts.LabelOpts(is_show=False),
        symbol_size=18,
        symbol_repeat="fixed",
        symbol_offset=[0, 0],
        is_symbol_clip=True,
        symbol=SymbolType.ROUND_RECT,
    )
    .reversal_axis()
    .set_global_opts(
        title_opts=opts.TitleOpts(title="PictorialBar-美国各州死亡人数Top10"),
        xaxis_opts=opts.AxisOpts(is_show=False),
        yaxis_opts=opts.AxisOpts(
            axistick_opts=opts.AxisTickOpts(is_show=False),
            axisline_opts=opts.AxisLineOpts(
                linestyle_opts=opts.LineStyleOpts(opacity=0)
            ),
        ),
    )
    .render("/home/hadoop/result/result5/result1.html")
    )
 
 
 
#6.找出美国确诊最少的10个州——>词云图
def drawChart_6(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            row=(str(js['state']),int(js['totalCases']))
            data.append(row)
 
    c = (
    WordCloud()
    .add("", data, word_size_range=[100, 20], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="美国各州确诊最少的10个州"))
    .render("/home/hadoop/result/result6/result1.html")
    )
 
 
 
 
#7.找出美国死亡最少的10个州——>漏斗图
def drawChart_7(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            data.insert(0,[str(js['state']),int(js['totalDeaths'])])
 
    c = (
    Funnel()
    .add(
        "State",
        data,
        sort_="ascending",
        label_opts=opts.LabelOpts(position="inside"),
    )
    .set_global_opts(title_opts=opts.TitleOpts(title=""))
    .render("/home/hadoop/result/result7/result1.html")
    )
 
 
#8.美国的病死率--->饼状图
def drawChart_8(index):
    root = "/home/hadoop/result/result" + str(index) +"/part-00000.json"
    values = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            if str(js['state'])=="USA":
                values.append(["Death(%)",round(float(js['deathRate'])*100,2)])
                values.append(["No-Death(%)",100-round(float(js['deathRate'])*100,2)])
    c = (
    Pie()
    .add("", values)
    .set_colors(["blcak","orange"])
    .set_global_opts(title_opts=opts.TitleOpts(title="全美的病死率"))
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
    .render("/home/hadoop/result/result8/result1.html")
    )
''' 
 
#可视化主程序：
index = 1
while index<2:
    funcStr = "drawChart_" + str(index)
    eval(funcStr)(index)
    index+=1