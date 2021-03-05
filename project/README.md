### 基于 TMDB 数据集的电影数据分析

- 数据集

    ```
    本次项目使用的数据集来自知名数据网站 Kaggle 的 tmdb-movie-metadata 电影数据集，该数据集包含大约 5000 部电影的相关数据。本次实验使用数据集中有关电影的数据表 tmdb_5000_movies.csv 进行实验。数据包含以下字段：
    字段名称 解释 例子
    budget 预算 10000000
    genres 体裁 “[{“”id””: 18, “”name””: “”Drama””}]”
    homepage 主页 “”
    id id 268238
    keywords 关键词 “[{“”id””: 14636, “”name””: “”india””}]”
    original_language 原始语言 en
    original_title 原标题 The Second Best Exotic Marigold Hotel
    overview 概览 As the Best Exotic Marigold Hotel …
    popularity 流行度 17.592299
    production_companies 生产公司 “[{“”name””: “”Fox Searchlight Pictures””, “”id””: 43}, …]”
    production_countries 生产国家 “[{“”iso31661″”: “”GB””, “”name””: “”United Kingdom””}, …]”
    release_date 发行日期 2015-02-26
    revenue 盈收 85978266
    runtime 片长 122
    spoken_languages 语言 “[{“”iso6391″”: “”en””, “”name””: “”English””}]”
    status 状态 Released
    tagline 宣传语 “”
    title 标题 The Second Best Exotic Marigold Hotel
    vote_average 平均分 6.3
    vote_count 投票人数 272

    由于数据中某些字段包含 json 数据，因此直接使用 DataFrame 进行读取会出现分割错误，所以如果要创建 DataFrame，需要先直接读取文件生成 RDD，再将 RDD 转为 DataFrame。过程中，使用 python3 中的 csv 模块对数据进行解析和转换。
    ```