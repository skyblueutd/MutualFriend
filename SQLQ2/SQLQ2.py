from pyspark import SparkContext, SparkConf, Row
import re
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def Friendlist(friend):
    me = friend[0]
    return [((me, other), friend[1])
            if me < other else ((other, me), friend[1])
            for other in friend[1]]


def mutualF(list):
    pair = list[0]
    setp = set({})
    res = []
    for friends in list[1]:
        if len(setp) == 0:
            for c in friends:
                setp.add(c)
        else:
            for c in friends:
                if c in setp:
                    res.append(c)
    return pair, res

spark = SparkSession.builder.appName("Q1SQL").config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext


    # generate all possible common friends pair
rdd0 = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: (x.split("\t")[0],x.split("\t")[1])).filter(lambda x: len(x[1]) != 0)
rdd1 = rdd0.map(lambda x : (x[0], x[1].split(",")))
rdd2 = rdd1.flatMap(Friendlist)
    # group values by common key
rdd3 = rdd2.groupByKey()
    # get top10 common friend pair that has the most number of common friends
rdd4 = rdd3.map(mutualF).filter(lambda x: len(x[1]) > 0)

    # use customized function to get len of common friends
countlen = udf(lambda x : len(x), IntegerType())
    # sort key value pairs by number of common friends and get the top10 as list
topList = spark.createDataFrame(rdd4).select("_1", countlen("_2").alias("_2")).sort("_2", ascending=False).head(10)
    # convert top10 list into df
tempTop10 = sc.parallelize(topList).map(lambda x: Row(urid=x[0][0], other=x[0][1]))
top10 = spark.createDataFrame(tempTop10)

userdata = sc.textFile("userdata.txt")
userdatafr = userdata.map(lambda x: re.split(r",", x)).map(lambda x: Row(address=x[3], lastname=x[2], firstname=x[1], urid2=x[0]))
userinfo = spark.createDataFrame(userdatafr)

    # join userinfo and left friend id
restemp = top10.join(userinfo, top10.urid == userinfo.urid2, "cross")\
        .selectExpr("urid2 as urid", "firstname as fname", "lastname as lname", "address as addr", "other as other")

    # join userinfo and right friend id
res = restemp.join(userinfo, restemp.other == userinfo.urid2, "cross").drop("urid2"). \
        selectExpr("urid as urid", "fname as fname", "lname as lname", "addr as addr", "other as other",\
                   "firstname as firstname", "lastname as lastname", "address as address")

res.repartition(1).write.save('./result1', 'csv', 'overwrite')