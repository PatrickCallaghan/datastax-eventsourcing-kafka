create keyspace if not exists datastax WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

use datastax;

CREATE TABLE IF NOT EXISTS eventsource (
date text,
bucket int,
id uuid, 
aggregatetype text, 
host text,
loglevel text,
data text, 
time timestamp, 
eventtype text, 
PRIMARY KEY((date, bucket), time, id)
);