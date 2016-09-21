from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys

files="hdfs://ec2-52-45-61-78.compute-1.amazonaws.com:9000/camus/topics/textticks/hourly/*/*/*/*/*"
conf = SparkConf().setAppName("text events").set("spark.cores.max", "6")
sc = SparkContext(conf=conf) 
sqlContext = SQLContext(sc) 

json_format = [StructField("timestamp", StringType(), True),
               StructField("uid", StringType(), True),
               StructField("tags", ArrayType(StringType()), True)]

#df = sqlContext.read.json(filen, StructType(json_format))
df=sqlContext.read.json(files)

#do some calculations
TopicsTotalCount=df.map(lambda x: x['topic'])\
  .flatMap(lambda x: x)\
  .filter(lambda x: x != '')\
  .map(lambda x:(x,1))\
  .reduceByKey(lambda x,y: x+y)\
  .sortBy(lambda x:x[1], ascending=False)\
  .toDF()\
  .toDF('topic','count')

TopicsCountPerUser=df.map(lambda x: [(x['uid'],i) for i in x['topic']])\
  .flatMap(lambda x: x)\
  .filter(lambda x: x != '')\
  .map(lambda x:(x,1))\
  .reduceByKey(lambda x,y: x+y)\
  .map(lambda ((x,y),z):(x,y,z))\
  .sortBy(lambda x:x)\
  .toDF()\
  .toDF('uid','topic','count')

#save
TopicsTotalCount.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode('overwrite')\
  .options(table="topics_total_count_batch", keyspace="ad_flow")\
  .save()

TopicsCountPerUser.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode('overwrite')\
  .options(table="topics_per_user_count_batch", keyspace="ad_flow")\
  .save()
#save to cassandra
#def AddToCassandra_totalcountbatch_bypartition(d_iter):
#    from cqlengine import columns
#    from cqlengine.models import Model
#    from cqlengine import connection
#    from cqlengine.management import sync_table
#    
#    class topics_total_count_batch(Model):
#        topic = columns.Text(primary_key=True)
#        count = columns.Integer()
#        
#    host="ec2-52-45-61-78.compute-1.amazonaws.com" #cassandra seed node, TODO: do not hard code this
#    connection.setup([host], "ad_flow")
#    sync_table(topics_total_count_batch)
#    for d in d_iter:
#        topics_total_count_batch.create(**d)
#
#AddToCassandra_totalcountbatch_bypartition([])
#TopicsTotalCount.foreachPartition(AddToCassandra_totalcountbatch_bypartition)

#def AddToCassandra_usercountbatch_bypartition(d_iter):
#    from cqlengine import columns
#    from cqlengine.models import Model
#    from cqlengine import connection
#    from cqlengine.management import sync_table
#    
#    class topics_per_user_count_batch(Model):
#        uid = columns.Text(primary_key=True)
#        topic = columns.Text(primary_key=True)
#        count = columns.Integer()
#        
#    host="ec2-52-45-61-78.compute-1.amazonaws.com" #cassandra seed node, TODO: do not hard code this
#    connection.setup([host], "ad_flow")
#    sync_table(topics_per_user_count_batch)
#    for d in d_iter:
#        TopicsTotalCount.create(**d)

#AddToCassandra_usercountbatch_bypartition([])
#TopicsCountPerUser.foreachPartition(AddToCassandra_usercountbatch_bypartition)

