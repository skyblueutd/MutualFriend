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


def swap(kval):
    return kval[1],kval[0]


def freq(keyval):
    freq = keyval[0]
    pair = keyval[1]
    return [(pair[0], (pair[1], freq)), (pair[1], (pair[0], freq))]

def merge(lst1, lst2):
    pair_info = str(lst1[0]) + "\t"
    for info in lst1[1]: pair_info = pair_info + info + "\t"
    for info in lst2[1]: pair_info = pair_info + info + "\t"
    return pair_info


def TopPair(keyval):
    key1 = keyval[0]
    key2 = keyval[1][0][0]
    key = (key1, key2) if key1 < key2 else (key2, key1)
    return (key, (keyval[1][0][1], keyval[1][1][0:3]))


if __name__ == '__main__':
    conf = SparkConf().setAppName("Top pair").setMaster("local")
    sc = SparkContext(conf=conf)

    friendList = sc.textFile("soc-LiveJournal1Adj.txt")
    ftemp = friendList.map(lambda x: (x.split("\t")[0],x.split("\t")[1])).filter(lambda x: len(x[1]) != 0)
    graph = ftemp.map(lambda x : (x[0], x[1].split(",")))
    commonFriends = graph.flatMap(lambda conn: FriendsConne(conn)).reduceByKey(MFriends)
    topFriends = sc.parallelize(commonFriends.map(lambda kval:swap(kval)).sortByKey(0).map(lambda kval: swap(kval), 1).take(10))
    #read userdata.txt
    userdata = sc.textFile("userdata.txt").map(lambda x: (x.split(",")[0], x.split(",")[1:]))
    #transform topFriends list so that it can join with userdata
    topPair = topFriends.map(lambda keyval: swap(keyval)).flatMap(lambda keyval: freq(keyval))
    pairInfo = topPair.join(userdata).map(lambda keyval: TopPair(keyval))
    pairInfo = pairInfo.reduceByKey(merge, 1).map(lambda keyval: keyval[1])
    pairInfo.saveAsTextFile("/home/xin/HW2Q2")