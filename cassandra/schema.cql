CREATE KEYSPACE ad_flow WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE ad_flow;


CREATE TABLE totalbalance (
    pid text, 
    price text, 
    count int, 
    cost float,    
    PRIMARY KEY (uid)
);

CREATE TABLE ad_flow.winningbid10s (
    pid text, 
    ts timestamp, 
    price float, 
    PRIMARY KEY (pid)
) WITH CLUSTERING ORDER BY (ts DESC);; 

CREATE TABLE ad_flow.pidscore1h (
    pid text, 
    ts timestamp, 
    scoree float, 
    PRIMARY KEY (pid)
) WITH CLUSTERING ORDER BY (ts DESC) and default_time_to_live = 3600; 

CREATE TABLE ad_flow.history1h (
    pid text,
    state text,
    ts timestamp,
    score float,
    count int,
    cost float,
    PRIMARY KEY (pid, state)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE ad_flow.bidprice (
    pid text, 
    price float, 
    PRIMARY KEY (pid)
); 
