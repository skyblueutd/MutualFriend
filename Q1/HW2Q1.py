from pyspark import SparkContext, SparkConf


def FriendsConne(friend):
    me = friend[0]
    friends_list = list(set(friend[1]))

    connections = [((other, me), friend[1])
                       if me > other else ((me, other), friend[1])
                       for other in friends_list]
    return connections


def MFriends(lst1, lst2):
    friends = dict()
    for each_friend in lst1:
        friends[each_friend] = 1 if each_friend not in friends else friends[each_friend] + 1

    for each_friend in lst2:
        friends[each_friend] = 1 if each_friend not in friends else friends[each_friend] + 1

    mutuals = []
    for key in friends:
        if friends[key] == 2:
            mutuals.append(key)
    return len(mutuals)

def mapresult(keyvalue):
    key,val = keyvalue
    key1, key2 = key
    return key1 + "," + key2 + ":\t" +str(val)


conf = SparkConf().setAppName("HW2").setMaster("local")
sc = SparkContext(conf=conf)

data = sc.textFile("soc-LiveJournal1Adj.txt")
Flist = data.map(lambda x: (x.split("\t")[0],x.split("\t")[1])).filter(lambda x: len(x[1]) != 0)
Mutual = Flist.map(lambda x : (x[0], x[1].split(",")))
commonfriends = Mutual.flatMap(lambda friend: FriendsConne(friend)).reduceByKey(MFriends)
commonfriends.map(lambda kv: mapresult(kv)).saveAsTextFile("/home/xin/HW2Q1")