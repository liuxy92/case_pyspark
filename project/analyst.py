from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructField, StructType
import json
import csv
import os

sc = SparkContext('local', 'spark_project')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

schemaString = "budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count"
fields = [StructField(field, StringType(), True)
          for field in schemaString.split(",")]
schema = StructType(fields)

moviesRdd = sc.textFile('tmdb_5000_movies.csv').map(
    lambda line: Row(*next(csv.reader([line]))))
mdf = spark.createDataFrame(moviesRdd, schema)


def countByJson(field):
    return mdf.select(field).filter(mdf[field] != '').rdd.flatMap(lambda g: [(v, 1) for v in map(lambda x: x['name'], json.loads(g[field]))]).repartition(1).reduceByKey(lambda x, y: x + y)

# 体裁统计


def countByGenres():
    res = countByJson("genres").collect()
    return list(map(lambda v: {"genre": v[0], "count": v[1]}, res))

# 关键词词云


def countByKeywords():
    res = countByJson("keywords").sortBy(lambda x: -x[1]).take(100)
    return list(map(lambda v: {"x": v[0], "value": v[1]}, res))

# 公司电影产出数量


def countByCompanies():
    res = countByJson("production_companies").sortBy(lambda x: -x[1]).take(10)
    return list(map(lambda v: {"company": v[0], "count": v[1]}, res))

# 预算统计


def countByBudget(order='count', ascending=False):
    return mdf.filter(mdf["budget"] != 0).groupBy("budget").count().orderBy(order, ascending=ascending).toJSON().map(lambda j: json.loads(j)).take(10)

# 语言统计


def countByLanguage():
    res = countByJson("spoken_languages").filter(
        lambda v: v[0] != '').sortBy(lambda x: -x[1]).take(10)
    return list(map(lambda v: {"language": v[0], "count": v[1]}, res))

# 电影时长分布 > 100 min


def distrbutionOfRuntime(order='count', ascending=False):
    return mdf.filter(mdf["runtime"] != 0).groupBy("runtime").count().filter('count>=100').toJSON().map(lambda j: json.loads(j)).collect()

# 预算评价关系


def budgetVote():
    return mdf.select("title", "budget", "vote_average").filter(mdf["budget"] != 0).filter(mdf["vote_count"] > 100).collect()

# 上映时间评价关系


def dateVote():
    return mdf.select(mdf["release_date"], "vote_average", "title").filter(mdf["release_date"] != "").filter(mdf["vote_count"] > 100).collect()

# 流行度评价关系


def popVote():
    return mdf.select("title", "popularity", "vote_average").filter(mdf["popularity"] != 0).filter(mdf["vote_count"] > 100).collect()


# 电影数量和评价的关系
def moviesVote():
    return mdf.filter(mdf["production_companies"] != '').filter(mdf["vote_count"] > 100).rdd.flatMap(lambda g: [(v, [float(g['vote_average']), 1]) for v in map(lambda x: x['name'], json.loads(g["production_companies"]))]).repartition(1).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]]).map(lambda v: [v[0], v[1][0] / v[1][1], v[1][1]]).collect()

# 预算和营收的关系
def budgetRevenue():
    return mdf.select("title", "budget", "revenue").filter(mdf["budget"] != 0).filter(mdf['revenue'] != 0).collect()

def save(path, data):
  with open(path, 'w') as f:
    f.write(data)

if __name__ == "__main__":
    m = {
        "countByGenres": {
            "method": countByGenres,
            "path": "genres.json"
        },
        "countByKeywords": {
            "method": countByKeywords,
            "path": "keywords.json"
        },
        "countByCompanies": {
            "method": countByCompanies,
            "path": "company_count.json"
        },
        "countByBudget": {
            "method": countByBudget,
            "path": "budget.json"
        },
        "countByLanguage": {
            "method": countByLanguage,
            "path": "language.json"
        },
        "distrbutionOfRuntime": {
            "method": distrbutionOfRuntime,
            "path": "runtime.json"
        },
        "budgetVote": {
            "method": budgetVote,
            "path": "budget_vote.json"
        },
        "dateVote": {
            "method": dateVote,
            "path": "date_vote.json"
        },
        "popVote": {
            "method": popVote,
            "path": "pop_vote.json"
        },
        "moviesVote": {
            "method": moviesVote,
            "path": "movies_vote.json"
        },
        "budgetRevenue": {
            "method": budgetRevenue,
            "path": "budget_revenue.json"
        }
    }
    base = "static/"
    if not os.path.exists(base):
        os.mkdir(base)

    for k in m:
        p = m[k]
        f = p["method"]
        save(base + m[k]["path"], json.dumps(f()))
        print ("done -> " + k + " , save to -> " + base + m[k]["path"])
    # save("test.jj", json.dumps(countByGenres()))
