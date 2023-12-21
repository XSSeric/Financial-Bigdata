# coding:utf8
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName('WordcountHelloworld').setMaster('local[*]')
    # 通过SparkConf设置配置信息，并创建SparkContext对象
    sc = SparkContext(conf=conf)
    # 读取hdfs上wordcount的单词计数文件word.txt，统计每个单词出现的次数
    lines = sc.textFile('../data/words.txt')
    # 对读取的文件内容进行单词拆分并计数为1
    words = lines.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1))
    # 对单词进行计数
    wordCounts = words.reduceByKey(lambda a, b: a + b)
    # 打印结果
    print(wordCounts.collect())
