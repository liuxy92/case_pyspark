# -*- coding: utf-8 -*-
import plotly.express as px
import plotly.io as pio
import pandas as pd
import numpy as np
 
data = pd.read_csv('earthquake1.csv')

# 1.数据总揽
fig1 = px.scatter_geo(data,
                      color = data.Magnitude,
                      color_continuous_scale = px.colors.sequential.Inferno,
                      lon = data.Longitude,
                      lat = data.Latitude,
                      hover_name = data.Type,
                      hover_data = ["Longitude", 
                                    "Latitude",
                                    "Date", 
                                    "Time",
                                    "Magnitude",
                                    "Depth" 
                                   ],
                      size = np.exp(data.Magnitude)/100,
                      projection = "equirectangular",
                      title = '1965-2016年全球重大地震'
                      )
fig1.show()
pio.write_html(fig1, 'fig1.html')

fig2 = px.scatter_geo(data,
                      color = data.Magnitude,
                      color_continuous_scale = px.colors.sequential.Inferno,
                      lon = data.Longitude,
                      lat = data.Latitude,
                      animation_frame = data.Year,
                      hover_name = data.Type,
                      hover_data = ["Longitude", 
                                    "Latitude",
                                    "Date", 
                                    "Time",
                                    "Magnitude",
                                    "Depth" 
                                   ],
                      size = np.exp(data.Magnitude)/100,
                      projection = "equirectangular",
                      title = '1965-2016年全球重大地震'
                      )
fig2.show()
pio.write_html(fig2, 'fig2.html')


# 2. 每年份、月份、天份发生重大地震的次数
cntByYear = pd.read_csv('countByYear.csv')
fig3 = px.bar(cntByYear,
              x = "Year",
              y = "count",
              text = "count", 
              title = '1965-2016年每年发生重大地震的次数'
             )
fig3.show()
pio.write_html(fig3, 'fig3.html')

cntByMonth = pd.read_csv('countByMonth.csv')
fig4 = px.bar(cntByMonth,
              x = "Month",
              y = "count",
              text = "count", 
              title = '1965-2016年每月发生重大地震的次数'
             )
fig4.show()
pio.write_html(fig4, 'fig4.html')

cntByDay = pd.read_csv('countByDay.csv')
fig5 = px.bar(cntByDay,
              x = "Day",
              y = "count",
              text = "count", 
              title = '1965-2016年每个日期发生重大地震的次数'
             )
fig5.show()
pio.write_html(fig5, 'fig5.html')


# 3. 1955-2016年中国境内不同省份的重大地震次数
dataC = pd.read_csv('earthquakeC.csv')
fig6 = px.scatter_geo(dataC,
                      color = dataC.Magnitude,
                      color_continuous_scale = px.colors.sequential.Inferno,
                      lon = dataC.Longitude,
                      lat = dataC.Latitude,
                      scope = 'asia',
                      center = {'lon': 105.73, 'lat': 29.6},
                      hover_name = dataC.Type,
                      hover_data = ["Longitude", 
                                    "Latitude",
                                    "Date", 
                                    "Time",
                                    "Magnitude",
                                    "Depth" 
                                   ],
                      size = np.exp(dataC.Magnitude)/100,
                      title = '1965-2016年中国境内重大地震'
                      )
fig6.show()
pio.write_html(fig6, 'fig6.html')

cntByArea = pd.read_csv('countByArea.csv')
fig7 = px.bar(cntByArea,
              x = "Area",
              y = "count",
              text = "count", 
              title = '1965-2016年各省份（海域）发生重大地震的次数'
             )
fig7.show()
pio.write_html(fig7, 'fig7.html')


# 4. 震级前500的重大地震
pow500 = pd.read_csv('mostPow.csv')
 
fig8 = px.scatter_geo(pow500,
                      color = pow500.Magnitude,
                      color_continuous_scale = px.colors.sequential.Inferno,
                      lon = pow500.Longitude,
                      lat = pow500.Latitude,
                      hover_name = pow500.Type,
                      hover_data = ["Longitude", 
                                    "Latitude",
                                    "Date", 
                                    "Time",
                                    "Magnitude",
                                    "Depth" 
                                   ],
                      size = np.exp(pow500.Magnitude)/100,
                      title = '1965-2016年震级前500的重大地震'
                      )
fig8.show()
pio.write_html(fig8, 'fig8.html')


# 5. 震源深度前500的重大地震
deep500 = pd.read_csv('mostDeep.csv')
 
fig9 = px.scatter_geo(deep500,
                      color = deep500.Depth,
                      color_continuous_scale = px.colors.sequential.Inferno,
                      lon = deep500.Longitude,
                      lat = deep500.Latitude,
                      hover_name = deep500.Type,
                      hover_data = ["Longitude", 
                                    "Latitude",
                                    "Date", 
                                    "Time",
                                    "Magnitude",
                                    "Depth" 
                                   ],
                      title = '1965-2016年震源深度前500的重大地震'
                      )
fig9.show()
pio.write_html(fig9, 'fig9.html')


# 6. 不同类型的地震
cntByType = pd.read_csv('countByTypeC.csv')
fig10 = px.pie(cntByType,
               names = "Type",
               values = "count",
               title = '1965-2016年世界范围内不同类型的地震占比'
              )
fig10.show()
pio.write_html(fig10, 'fig10.html')

cntByTypeC = pd.read_csv('countByTypeC.csv')
fig11 = px.pie(cntByTypeC,
               names = "Type",
               values = "count",
               title = '1965-2016年中国境内不同类型的地震占比'
              )
fig11.show()
pio.write_html(fig11, 'fig11.html')


# 7. 震级和震源深度的关系
powAndDep = pd.read_csv('powDeep.csv')
 
fig12 = px.scatter(powAndDep,
                   x = "Depth",
                   y = "Magnitude",
                   title = '震级与震源深度的关系'
                  )
fig12.show()
pio.write_html(fig12, 'fig12.html')