# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template
from app import app
import datetime
from dateutil.parser import parse

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-52-54-224-184.compute-1.amazonaws.com'])
#cluster = Cluster(['172.31.0.36'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('ad_flow')

@app.route('/')
@app.route('/index')
def index():
  return render_template("base.html")

@app.route('/api/<pid>')
def get_score(pid):
   pid='pid'+pid.zfill(5)
   queryDB = "SELECT ts as ts, score FROM pidscore1h WHERE pid=%s"
   response = session.execute(queryDB, parameters=[pid])
   response_list = []
   for val in response:
       response_list.append(val)
   jsonresponse = [{"score": x.score, "time": getDtSeconds(x.ts)} for x in response_list]
   return jsonify(pidscore=jsonresponse)

@app.route('/score/<pid>')
def get_max_score(pid):
   queryDB = "SELECT pid, max(ts) as ts, score FROM pidscore1h WHERE pid=%s"
   response = session.execute(queryDB, parameters=[pid])
   response_list = []
   for val in response:
       response_list.append(val)
   jsonresponse = [{"score": x.score, "time": getDtSeconds(x.ts)} for x in response_list]
   return jsonify(maxscore=jsonresponse)

@app.route('/map/<pid>')
def get_map(pid):
   pid='pid'+pid.zfill(5)
   queryDB = "SELECT state, count, cost FROM dashboardmap where pid=%s"
   response = session.execute(queryDB, parameters=[pid])
   response_list = []
   for val in response:
       response_list.append(val)
   jsonresponse = [{"code": x.state, "value": x.count, "cost":x.cost} for x in response_list]
   return jsonify(statecount=jsonresponse)
  
@app.route('/map')
def email():
 return render_template("map.html")

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")

@app.route('/test')
def test():
 return render_template("test.html")

@app.route('/bids/<pid>')
def get_bids(pid):
   queryDB = "SELECT pid, uid, ts, price FROM winningbids limit 3000"# WHERE pid=%s"
   response = session.execute(queryDB, parameters=[])
   response_list = []
   for val in response:
       response_list.append(val)
   jsonresponse = [{"pid":x.pid, "uid":x.uid, "price": float('{0:.02f}'.format(x.price)), "time": x.ts.isoformat()} for x in response_list]
   return jsonify(pidscore=jsonresponse)

def getDtSeconds(dt1, dt0=None):
    """Get time different in seconds between two datetime objects
    INput:
    dt1: ending datetime.datetime object
    dt0: starting datetime.datetime object
    Output: difference in seconds
    """
    if dt0==None: dt0=datetime.datetime.utcfromtimestamp(0)
    return (dt1-dt0).total_seconds()*1000

@app.route('/revenue')
def get_revenue():
   queryDB = "SELECT ts,  cost FROM revenueflow"# WHERE pid=%s"
   response = session.execute(queryDB, parameters=[])
   response_list = []
   for val in response:
       response_list.append(val)
   jsonresponse = [{"time": getDtSeconds(x.ts), "revenue": float('{0:.02f}'.format(x.cost))} for x in response_list]
   def getKey(item):return item['time']
   jsonresponse=sorted(jsonresponse, key=getKey)
   return jsonify(revenue=jsonresponse)
