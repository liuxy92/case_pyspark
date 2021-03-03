# -*- coding: utf-8 -*-
from pyecharts.charts import Bar
from pyecharts.charts import Pie
from pyecharts.charts import Page
from pyecharts import options as opts
import data_analysis


# --------总体逾期人数情况--------------
def draw_total(total_list):
    attr = ["未逾期人数", "逾期人数"]
    pie = (
        Pie()
            .add("总体逾期人数", [list(z) for z in zip(attr,total_list)])
            .set_global_opts(title_opts=opts.TitleOpts(title="总体逾期人数分布"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
            )
    )
    return pie

# --------年龄与逾期人数情况--------------
def draw_age(age_list,y_ageList):
    total_pie = draw_total(all_list[0])
    attr = ["0-30", "30-45", "45-60", "60-75", "75-100"]
    y0_agenum = []
    y1_agenum = []
    for i in range(5):
        y0_agenum.append(y_ageList[i][0])
        y1_agenum.append(y_ageList[i][1])
 
    bar = (
        Bar()
            .add_xaxis(attr)
            .add_yaxis("人数分布", age_list)
            .add_yaxis("未逾期人数分布", y0_agenum)
            .add_yaxis("逾期人数分布", y1_agenum)
            .set_global_opts(title_opts=opts.TitleOpts(title="各年龄段逾期情况"))
    )
    attr = ["未逾期","逾期"]
    pie1 = (
        Pie()
            .add("0-30年龄段", [list(z) for z in zip(attr,y_ageList[0])])
            .set_global_opts(title_opts=opts.TitleOpts(title="0-30年龄段逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
            )
    )
    pie2 = (
        Pie()
            .add("30-45年龄段", [list(z) for z in zip(attr,y_ageList[1])])
            .set_global_opts(title_opts=opts.TitleOpts(title="30-45年龄段逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
            )
    )
    pie3 = (
        Pie()
            .add("45-60年龄段", [list(z) for z in zip(attr,y_ageList[2])])
            .set_global_opts(title_opts=opts.TitleOpts(title="45-60年龄段逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
            )
    )
    pie4 = (
        Pie()
            .add("60-75年龄段", [list(z) for z in zip(attr,y_ageList[3])])
            .set_global_opts(title_opts=opts.TitleOpts(title="60-75年龄段逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    pie5 = (
        Pie()
            .add("75-100年龄段", [list(z) for z in zip(attr,y_ageList[4])])
            .set_global_opts(title_opts=opts.TitleOpts(title="75-100年龄段逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
 
    page = Page()
    page.add(bar)
    page.add(total_pie)
    page.add(pie1)
    page.add(pie2)
    page.add(pie3)
    page.add(pie4)
    page.add(pie5)
    page.render('age_OverDue.html')
 
# --------逾期记录与逾期人数情况--------------
def draw_pastdue(numofpastdue,pastdue1num,pastdue2num,pastdue12num):
    total_pie = draw_total(all_list[0])
    attr = ["有30-59days逾期记录的人数", "有60-89days逾期记录的人数", "有长短期逾期记录的人数"]
    bar = (
        Bar()
            .add_xaxis(attr)
            .add_yaxis("人数", numofpastdue)
            .set_global_opts(title_opts=opts.TitleOpts(title="有逾期记录的人数"))
    )
    attr = ["未逾期","逾期"]
    pie1 = (
        Pie()
            .add("有短期逾期记录的人的逾期情况", [list(z) for z in zip(attr,pastdue1num)])
            .set_global_opts(title_opts=opts.TitleOpts(title="有短期逾期记录的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    pie2 = (
        Pie()
            .add("有长期逾期记录的人的逾期情况", [list(z) for z in zip(attr,pastdue2num)])
            .set_global_opts(title_opts=opts.TitleOpts(title="有长期逾期记录的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    pie3 = (
        Pie()
            .add("长短期逾期记录都有的人的逾期情况", [list(z) for z in zip(attr,pastdue12num)])
            .set_global_opts(title_opts=opts.TitleOpts(title="长短期逾期记录都有的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    page = Page()
    page.add(bar)
    page.add(total_pie)
    page.add(pie1)
    page.add(pie2)
    page.add(pie3)
    page.render('pastDue_OverDue.html')

# --------房产抵押与逾期人数情况--------------
def draw_realestateLoans(numofrealornoreal,y_norealnum,y_realnum):
    total_pie = draw_total(all_list[0])
    attr = ["无房产抵押人数", "有房产抵押人数"]
    bar = (
        Bar()
            .add_xaxis(attr)
            .add_yaxis("人数", numofrealornoreal)
            .set_global_opts(title_opts=opts.TitleOpts(title="房产抵押人数分布"))
    )
    attr = ["未逾期","逾期"]
    pie1 = (
        Pie()
            .add("无房产抵押的人的逾期情况", [list(z) for z in zip(attr,y_norealnum)])
            .set_global_opts(title_opts=opts.TitleOpts(title="无房产抵押的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    pie2 = (
        Pie()
            .add("有房产抵押的人的逾期情况", [list(z) for z in zip(attr,y_realnum)])
            .set_global_opts(title_opts=opts.TitleOpts(title="有房产抵押的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    page = Page()
    page.add(bar)
    page.add(total_pie)
    page.add(pie1)
    page.add(pie2)
    page.render('realestateLoans_OverDue.html')
 
# --------家属人数与逾期人数情况--------------
def draw_families(nofamiliesAndfamilies,y_families,y_nofamilies):
    total_pie = draw_total(all_list[0])
    attr = ["有家属人数", "无家属人数"]
    bar = (
        Bar()
            .add_xaxis(attr)
            .add_yaxis("人数", nofamiliesAndfamilies)
            .set_global_opts(title_opts=opts.TitleOpts(title="有无家属人数分布"))
    )
    attr = ["未逾期","逾期"]
    pie1 = (
        Pie()
            .add("无家属的人的逾期情况", [list(z) for z in zip(attr,y_nofamilies)])
            .set_global_opts(title_opts=opts.TitleOpts(title="无家属的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    pie2 = (
        Pie()
            .add("有家属的人的逾期情况", [list(z) for z in zip(attr,y_families)])
            .set_global_opts(title_opts=opts.TitleOpts(title="有家属的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    page = Page()
    page.add(bar)
    page.add(total_pie)
    page.add(pie1)
    page.add(pie2)
    page.render('families_OverDue.html')

# --------月收入与逾期人数情况--------------
def draw_income(numofMeanincome,y_NoMeanIncome,y_MeanIncome):
    total_pie = draw_total(all_list[0])
    attr = ["未超过均值收入人数", "超过均值收入人数"]
    bar = (
        Bar()
            .add_xaxis(attr)
            .add_yaxis("人数", numofMeanincome)
            .set_global_opts(title_opts=opts.TitleOpts(title="有无超过均值收入人数分布"))
    )
    attr = ["未逾期","逾期"]
    pie1 = (
        Pie()
            .add("未超过均值收入的人的逾期情况", [list(z) for z in zip(attr,y_NoMeanIncome)])
            .set_global_opts(title_opts=opts.TitleOpts(title="未超过均值收入的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    pie2 = (
        Pie()
            .add("超过均值收入的人的逾期情况", [list(z) for z in zip(attr,y_MeanIncome)])
            .set_global_opts(title_opts=opts.TitleOpts(title="超过均值收入的人的逾期情况"))
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"),
            label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)")
        )
    )
    page = Page()
    page.add(bar)
    page.add(total_pie)
    page.add(pie1)
    page.add(pie2)
    page.render('meanIncome_OverDue.html')
 

if __name__ == '__main__':
    print("开始总程序")
    Filename = "pyspark/OverDue/data.csv"
    all_list = data_analysis.analyse(Filename) 
    # 年龄与是否逾期情况
    draw_age(all_list[1],all_list[2])
    # 有无逾期记录与是否逾期情况
    draw_pastdue(all_list[3],all_list[4],all_list[5],all_list[6])
    # 房产抵押数量与是否逾期情况
    draw_realestateLoans(all_list[7],all_list[8],all_list[9])
    # 家属人数与是否逾期情况
    draw_families(all_list[10],all_list[11],all_list[12])
    # 月收入与是否逾期情况
    draw_income(all_list[13],all_list[14],all_list[15])
    print("结束总程序")