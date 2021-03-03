### 基于Spark的地震数据处理与分析

- 安装可视化所需要的软件包，在linux终端输入一下命令即可：

    ```
    sudo apt-get install python3-matplotlib
    sudo apt-get install python3-pandas
    sudo apt-get install python3-mpltoolkits.basemap 
    ```

- 数据准备：

    数据来自和鲸社区的1965-2016全球重大地震数据，文件名为earthquake.csv包括23412条地震数据，其中有很多属性大部分缺失且本次实验用不到，将其手动删除，只保留Date, Time, Latitude, Longitude, Type, Depth, Magnitude这七个属性。  
    数据来自和鲸社区的1965-2016全球重大地震数据，文件名为earthquake.csv包括23412条地震数据，其中有很多属性大部分缺失且本次实验用不到，将其手动删除，只保留Date, Time, Latitude, Longitude, Type, Depth, Magnitude这七个属性。

- 数据清洗：

    preprocessing.py

- 数据分析及可视化

    analyze.py