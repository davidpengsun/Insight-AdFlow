# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template
from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-52-2-60-169.compute-1.amazonaws.com'])
#cluster = Cluster(['172.31.0.36'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('ad_flow')

@app.route('/')
@app.route('/index')
def index():
  return render_template("base.html")

@app.route('/api/<pid>')
def get_score(pid):
   queryDB = "SELECT pid, ts as ts, score FROM pidscore1h WHERE pid=%s"
   response = session.execute(queryDB, parameters=[pid])
   response_list = []
   for val in response:
       response_list.append(val)
   jsonresponse = [{"pid": x.pid, "score": x.score, "time": x.ts} for x in response_list]
   return jsonify(pidscore=jsonresponse)

@app.route('/map')
def email():
 return render_template("map.html")

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")
