import findspark
findspark.init('/usr/local/Cellar/spark-2.4.5-bin-hadoop2.7/')
from pyspark import SparkContext
sc = SparkContext(appName="getEvenNums")
words = sc.parallelize(["scala","java","hadoop","spark","akka"])
print(words.count())
print(sc)