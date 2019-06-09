import os

from pyspark import SparkConf, SparkContext


def reduceLine(x):
    site = x[0]
    locations = x[1]
    locDict = {}
    for loc in locations:
        if loc in locDict:
            locDict[loc] += 1
        else:
            locDict[loc] = 1

    sortLocs = sorted(locDict.items(), key=lambda kv: kv[1], reverse=True)

    if len(sortLocs) < 2:
        return site, sortLocs

    return site, sortLocs[:2]


if __name__ == '__main__':
    # 统计每个网站最活跃的top2地区
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    conf = SparkConf().setMaster("local").setAppName("test")
    sc = SparkContext(conf=conf)
    fPath = BASE_DIR + r'\learn\new.text'
    lines = sc.textFile(fPath)
    print(fPath)
    res = lines.map(lambda line: (line.split("\t")[4], line.split("\t")[3])).groupByKey().map(lambda x: reduceLine(x))
    for re in res.collect():
        print(res+'\n')

