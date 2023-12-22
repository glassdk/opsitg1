from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('wordcounts').master('local[*]').getOrCreate()
df = spark.read.csv('hdfs://localhost:8020/data/sample.txt',sep='|')
df1 = df.select(explode(split('_c0',' ')).alias('words'))
df2 = df1.groupBy('words').count()
df2.write.csv('hdfs://localhost:8020/data/samp1/')
