from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class simpleWordCount:
    sc = ""
    def __init__(self):
        super().__init__()
        self.sc = SparkContext.getOrCreate()
    
    def word(self, path):
        words = self.sc.textFile(path).flatMap(lambda line: line.split(" "))
        wordCounts = words.map(lambda word:(word, 1)).reduceByKey(lambda a, b: a+b) 
        wordCounts.saveAsTextFile("result.txt")
        
        
        
    