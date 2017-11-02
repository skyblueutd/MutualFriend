from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


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
    return pair[0],pair[1],res


session = SparkSession.builder.appName("Q1SQL").config("spark.some.config.option", "some-value").getOrCreate()
sc = session.sparkContext
# generate all possible common friends pair
rdd0 = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: (x.split("\t")[0],x.split("\t")[1])).filter(lambda x: len(x[1]) != 0)
rdd1 = rdd0.map(lambda x : (x[0], x[1].split(",")))
rdd2 = rdd1.flatMap(Friendlist)

# group values by common key
rdd3 = rdd2.groupByKey()
rdd4 = rdd3.map(mutualF).filter(lambda x: len(x[2]) > 0)
# sort key value pairs by number of common friends
df = session.createDataFrame(rdd4)
countlen = udf(lambda x : len(x), IntegerType())
result = df.select("_1","_2",countlen("_3")).sort(["_1","_2"]).repartition(1).write.save('./result', 'csv', 'overwrite')