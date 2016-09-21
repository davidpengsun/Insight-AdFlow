CREATE KEYSPACE Ad_Flow WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE Ad_Flow;

CREATE TABLE topics_total_count_batch (topic text, count int, PRIMARY KEY (topic));

CREATE TABLE topics_per_user_count_batch (uid text, topic text, count int, PRIMARY KEY (uid, topic));

