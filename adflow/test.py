
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-52-2-60-169.compute-1.amazonaws.com'])
#cluster = Cluster(['172.31.0.36'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('ad_flow')

stmt = "SELECT pid, max(ts) as ts, score FROM pidscore1h WHERE pid=%s"
response = session.execute(stmt, parameters=['pid00333'])
response_list = []
for val in response:
    response_list.append(val)
    print(val)

