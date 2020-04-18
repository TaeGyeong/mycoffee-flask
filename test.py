import pyspark
import pyspark.streaming

SparkContext = pyspark.SparkContext("local[2]", "NetworkWordCount")
StreamingContext = pyspark.streaming.StreamingContext(SparkContext, 1)

lines = StreamingContext.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line:line.split(" "))


pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y : x+y)

wordCounts.pprint()

StreamingContext.start()
StreamingContext.awaitTermination()