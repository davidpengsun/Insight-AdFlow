from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

conf = SparkConf().setAppName("test")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

total_count_batch=sql.read.format("org.apache.spark.sql.cassandra").\
               load(keyspace="ad_flow", table="topics_total_count_batch")

total_count_batch.where(col("count")>100)\
		 .show()
