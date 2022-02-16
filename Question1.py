import findspark
findspark.init()

import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext

def pair_user_friends(user_friendslist):
	fList = user_friendslist[1]
	return [(pair_of_usersFriend, 1) for pair_of_usersFriend in it.combinations(fList, 2)]

def mergeByShareCount(d1, d2):
	for k2 in d2:
		if k2 in d1:
			d1[k2] += d2[k2]
		else:
			d1[k2] = d2[k2]
	return d1

def sortRecommendsByCount(rTuple):
	rlist = []
	rdict = rTuple[1]
	ckeys = sorted(rdict, reverse=True)
	for cnt in ckeys:
	    rlist.append((cnt, sorted(rdict[cnt])))
	return (rTuple[0], rlist)

def recList(pairs):
    rlist = []
    p = pairs[0]
    cfList = pairs[1]
    flen = len(cfList)

    for i in range(flen):
        for j in range(len(cfList[i][1])):
            rlist.append(cfList[i][1][j])
    return (p, rlist)

def pMap2file(pMap, pList, N, fname):
	fp = open(fname, 'w+')
	for k in pList:
		if k in pMap:
			plen = min(len(pMap[k]), N)
			s = ','.join(str(e) for e in pMap[k][:plen])
			print(fp, k, '\t', s, file=fp)
            
            
		else:
			print (fp, k, file=fp)
        
	fp.close()

conf = SparkConf()
sc = SparkContext(conf=conf)

# read text file
lines = sc.textFile('soc-LiveJournal1Adj.txt')

user_friends = lines.map(lambda l: l.split())
user  = user_friends.map(lambda x: x[0])

Users = user.collect()
Users = map(int, Users)

user_w_friends = user_friends.filter(lambda e: len(e) == 2)
user_friendList = user_w_friends.map(lambda p: (int(p[0]), map(int, sorted(p[1].split(',')))))

pairs_from_commonFriend = user_friendList.map(pair_user_friends).flatMap(lambda x: x)
pairs_CntOf_commonFriends = pairs_from_commonFriend.reduceByKey(lambda c1, c2: c1 + c2)
user2friendsMap = user_friendList.collectAsMap()
pairs_hasCommon_yetFriends = pairs_CntOf_commonFriends.filter(lambda pC: pC[0][1] not in user2friendsMap[pC[0][0]])
user_recommendList_byShareCnt = pairs_hasCommon_yetFriends.map(lambda pC: [(pC[0][0], {pC[1]: [pC[0][1]]}), (pC[0][1], {pC[1]: [pC[0][0]]})]).flatMap(lambda x: x)
recommendList_byShareCnt = user_recommendList_byShareCnt.reduceByKey(mergeByShareCount)
recommends = recommendList_byShareCnt.map(sortRecommendsByCount)
recommendList = recommends.map(recList)
recommendMap = recommendList.collectAsMap()

uList = sorted(Users)
pMap2file(recommendMap, uList, 10, 'output.txt')

sc.stop()