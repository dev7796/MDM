import findspark
findspark.init()

import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext
with open('browsing.txt', 'r') as browsing:
	lines = browsing.readlines()

with open('marketBrowsingBasket.txt', 'w') as bbrowsing:
	for (num, line) in enumerate(lines):
		bbrowsing.write('%d:%s' % (num, line))

conf = SparkConf()
sc = SparkContext(conf=conf)

marketBasklines = sc.textFile('marketBrowsingBasket.txt')
basket_Items = marketBasklines.map(lambda l: l.split(':'))
basketItems = basket_Items.map(lambda p: (int(p[0]), sorted(p[1].split())))

# Find frequent items
def items_of_baskets(basket):
	itemList = basket[1]
	return [(item, 1) for item in itemList]

def item_pairs_of_basket(basket):
	itemList = basket[1]
	return [(pair_of_items, 1) for pair_of_items in it.combinations(itemList, 2)]

def frequent_singletons(basket):
	id = basket[0]
	list = basket[1]
	fList = []
	for item in list:
		if item in freqItems:
			fList.append(item)
	return (id, sorted(fList))

def frequentDoubletons(basket_freqSingles):
	pairList = []
	id = basket_freqSingles[0]
	frequent = basket_freqSingles[1]
	cList = [pC for pC in it.combinations(frequent, 2)]
	for pair in cList:
		if pair in freqPairs:
			pairList.append(pair)
	return (id, sorted(pairList))

def item_triples_of_basket(freqPairs):
	freq_items = []
	freq_pairs = freqPairs[1]
	for p in freq_pairs:
		i1, i2 = p
		if i1 not in freq_items:
			freq_items.append(i1)
		if i2 not in freq_items:
			freq_items.append(i2)
	return [(triples, 1) for triples in it.combinations(sorted(freq_items), 3)]

def rules_from_pairs_sort_by_conf(pairs, items):
	ruleList = []
	for pair in pairs:
		x = pair[0]
		y = pair[1]
		X_support = items[x]
		Y_support = items[y]
		XYsupport = pairs[pair]
		Conf_XtoY = (1.0 * XYsupport)/X_support
		Conf_YtoX = (1.0 * XYsupport)/Y_support
		ruleList.append((Conf_XtoY, (x, y)))
		ruleList.append((Conf_YtoX, (y, x)))
	return sorted(ruleList, key = lambda x: (-x[0], x[1]))

def rules_fromTriples_sortByConf(triples, pairs):
	ruleList = []
	for triple in triples:
		x = triple[0]
		y = triple[1]
		z = triple[2]
		XY_support = pairs[(x, y)]
		XZ_support = pairs[(x, z)]
		YZ_support = pairs[(y, z)]
		XYZsupport = triples[triple]
		Conf_XYtoZ = (1.0 * XYZsupport)/XY_support
		Conf_XZtoY = (1.0 * XYZsupport)/XZ_support
		Conf_YZtoX = (1.0 * XYZsupport)/YZ_support
		ruleList.append((Conf_XYtoZ, ((x, y), z)))
		ruleList.append((Conf_XZtoY, ((x, z), y)))
		ruleList.append((Conf_YZtoX, ((y, z), x)))
	return sorted(ruleList, key = lambda x: (-x[0], x[1]))

def pOne2OneRules2file(rList, N, fname):
	fp = open(fname, 'w+')
	for i in range(N):
		#print >> fp, "Conf: %.8f\tRule: %s => %s" % (rList[i][0], rList[i][1][0], rList[i][1][1])
		print ("Conf: %.8f\tRule: %s => %s" % (rList[i][0], rList[i][1][0], rList[i][1][1]), file=fp)
	fp.close()

def pTwo2OneRules2file(rList, N, fname):
	fp = open(fname, 'w+')
	# pList = sorted(pMap.keys())
	for i in range(N):
		#print >> fp, "Conf: %.8f\tRule: (%s, %s) => %s" % (rList[i][0], rList[i][1][0][0], rList[i][1][0][1], rList[i][1][1])
		print("Conf: %.8f\tRule: (%s, %s) => %s" % (rList[i][0], rList[i][1][0][0], rList[i][1][0][1], rList[i][1][1]), file=fp)
	fp.close()

threshold = 100
items = basketItems.map(items_of_baskets).flatMap(lambda x: x)
itemCounts = items.reduceByKey(lambda c1, c2: c1 + c2)
frequent_items = itemCounts.filter(lambda x: x[1] >= threshold)
freqItems = frequent_items.collectAsMap()

basket_freqSingletons = basketItems.map(frequent_singletons)

candidate_doubles = basket_freqSingletons.map(item_pairs_of_basket).flatMap(lambda x: x)
doubleCounts = candidate_doubles.reduceByKey(lambda c1, c2: c1 + c2)

frequent_pairs = doubleCounts.filter(lambda x: x[1] >= threshold)

freqPairs = frequent_pairs.collectAsMap()

basket_freqDoubletons = basket_freqSingletons.map(frequentDoubletons)

candidate_triples = basket_freqDoubletons.map(item_triples_of_basket).flatMap(lambda x: x)

tripleCounts = candidate_triples.reduceByKey(lambda c1, c2: c1 + c2)
frequent_triples = tripleCounts.filter(lambda x: x[1] >= threshold)

freqTriples = frequent_triples.collectAsMap()

one2oneRules = rules_from_pairs_sort_by_conf(freqPairs, freqItems)

topN = 5
for i in range(topN):
	print ("Conf: %.8f\tRule: %s => %s" % (one2oneRules[i][0], one2oneRules[i][1][0], one2oneRules[i][1][1]))

print ("\n\n")

two2oneRules = rules_fromTriples_sortByConf(freqTriples, freqPairs)

for i in range(topN):
	print ("Conf: %.8f\tRule: (%s, %s) => %s" % (two2oneRules[i][0], two2oneRules[i][1][0][0], two2oneRules[i][1][0][1], two2oneRules[i][1][1]))

pOne2OneRules2file(one2oneRules, len(one2oneRules), '1-to-1-rules.txt')
pTwo2OneRules2file(two2oneRules, len(two2oneRules), '2-to-1-rules.txt')

sc.stop()