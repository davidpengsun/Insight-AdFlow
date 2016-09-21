from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

# set up cassandra

conf = SparkConf().setAppName("test")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame([{"topic":"abc", "count":100}])

def writeOutDF(iterator): 
	from cqlengine import columns
	from cqlengine.models import Model
	from cqlengine import connection
	from cqlengine.management import sync_table
	class topics_total_count_batch(Model):
	  topic = columns.Text(primary_key=True)
	  count = columns.Integer()

	connection.setup(['172.31.0.40'], "ad_flow")
	sync_table(topics_total_count_batch)
	
	for x in iterator:
	  topics_total_count_batch.create(**x)

df.foreachPartition(writeOutDF)

# write df to cassandra
#print "Writing RDD to Cassandra (pyspark_cassandra)"
#df.write\
#  .format("org.apache.spark.sql.cassandra")\
#  .mode('append')\
#  .options(table="topics_total_count_batch", keyspace="ad_flow")\
#  .save()
