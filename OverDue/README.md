### 基于信用卡逾期数据的Spark数据处理与分析

- python包安装

    ```
    pip3 install pyecharts==1.7.0
    ```

- 数据集
    
    - 数据集下载
        
        ```
        本次实验数据集来自和鲸社区的信用卡评分模型构建数据，以数据集cs-training.csv为分析主体，其中共有15万条记录，11列属性。
        每个数据包含以下字段：
        字段名称 字段含义 例子
        （1）SeriousDlqin2yrs 是否逾期 0,1
        （2）RevolvingUtilizationOfUnsecuredLines 信用卡和个人信贷额度的总余额 0.766126609
        （3）Age 年龄 45,20,30
        （4）NumberOfTime30-59DaysPastDueNotWorse 借款人逾期30-59天的次数 0,2,3
        （5）DebtRatio 负债比率 0.802982129
        （6）MonthlyIncome 月收入 9120,3000
        （7）NumberOfOpenCreditLinesAndLoans 未偿还贷款数量 ,0,4,13
        （8）NumberOfTimes90DaysLate 借款人逾期90天以上的次数 0,1,3
        （9）NumberRealEstateLoansOrLines 房地产贷款的数量 3,6
        （10）NumberOfTime60-89DaysPastDueNotWorse 借款人逾期60-89天的次数 0,3
        （11）NumberOfDependents 家庭中的家属人数 0,1,3
        ```

    - 数据预处理

        ```
        本次实验采用pandas库对数据进行预处理。在实验中，不对信用卡和个人信贷额度的总余额、负债比率、未偿还贷款数量、逾期90天以上的次数这4个属性进行处理分析。
        具体处理步骤如下：
        （1）读取数据
        （2）查看数据是否具有重复值，去除重复值
        （3）查看各字段缺失率，缺失值以均值填充
        （4）选取要研究的属性，删除不研究的属性
        （5）保存文件到本地
        使用代码文件data_preprocessing.py对数据预处理，运行data_preprocessing.py文件的步骤如下：
        ```

    - 将文件上传至HDFS文件系统

        将本地文件系统的数据集“data.csv”上传到HDFS文件系统中，路径为“pyspark/OverDue/data.csv”。具体命令如下：
        ```
        # 启动Hadoop
        cd /usr/local/hadoop
        ./sbin/start-dfs.sh
        # 在HDFS文件系统中创建/OverDue目录
        ./bin/hdfs dfs -mkdir -p pyspark/OverDue/
        # 上传文件到HDFS文件系统中
        ./bin/hdfs dfs -put ${path}/data/data.csv pyspark/OverDue/data.csv
        ```

- 使用Spark对数据处理分析

    ```
    我们将采用Python编程语言和Spark大数据框架对数据集“data.csv”进行处理分析，具体步骤如下：
    （1）读取HDFS文件系统中的数据文件，生成DataFrame
    （2）修改列名
    （3）本次信用卡逾期的总体统计
    （4）年龄与本次信用卡逾期的结合统计
    （5）两次逾期记录与本次信用卡逾期的结合统计
    （6）房产抵押数量与本次信用卡逾期的结合统计
    （7）家属人数与本次信用卡逾期的结合统计
    （8）月收入与本次信用卡逾期的结合统计
    （9）将统计数据返回给数据可视化文件data_web.py
    代码文件data_analysis.py的内容如下：
    ```

- 数据可视化
    - 可视化工具和代码

        ```
        选择使用python第三方库pyecharts作为可视化工具，其中pyecharts版本为1.7.0。采用其中的柱状图和饼图来详细展现分析结果。
        代码文件data_web.py的内容如下：
        ```
    - 执行

        ```
        # 进入OverDue目录
        cd case_pyspark/OverDue
        # 提交data_web.py文件到spark-submit
        /usr/local/spark/bin/spark-submit --master local ./data_web.py
        ```