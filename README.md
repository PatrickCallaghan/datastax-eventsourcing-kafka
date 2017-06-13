Event Sourcing
========================

This demo shows how Cassandra and DSE can be using to store and replay events. 

To use Spark you will need to provide your own Cassandra and Spark deployments. In this demo we will use DSE as they are already integrated.

First we start DSE in SearchAnalyics mode to allow us to use both Spark and DSE Search - 
http://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/operations/startStop/startDseStandalone.html?hl=starting

The implementation uses bucketing to group all data into particular time buckets for replay. The time bucket used in this example is 1 minute but any time bucket can be used. Also depending how many days, months, years of events that need to be kept, it may be beneficial to spread the events over different tiers of tables.    

To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
	
To create the solr core to make our table searchable, run the following

	dsetool create_core datastax.eventsource generateResources=true
	
To create events, run the following (Default of 10 million events) 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.events.Main"  -DcontactPoints=localhost -DnoOfEvents=10000000
	
To replay a sample event set, run 

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.events.ReadEvents"  -DcontactPoints=localhost -Dfrom=yyyyMMdd-hhmmss -Dto=yyyyMMdd-hhmmss
	
eg

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.events.ReadEvents"  -DcontactPoints=localhost -Dfrom=20160805-000000 -Dto=20160805-010000
	
This replays 2 scenarios

	1. Replay all events for a specified time range
	2. Replay all events for a specified time range and a specific event type.		
			
To run the webservice

	mvn jetty:run -Djetty.port=8081 
	
To run a rest query, go the brower and enter a url in the format http://localhost:8080/datastax-eventsourcing/rest/getevents/from/to, 
where the date format is 'yyyyMMdd-hhmmss' e.g. For all events from midnight to 1:00 am on the 1st of August 2016 run - 

	http://localhost:8081/datastax-eventsourcing/rest/getevents/20160801-000000/20160801-010000/

We can also use cql to query using the Solr query from DSE Search

Get all LOGIN Events from 9th Aug 2016 at 12:30 to 11th Aug 2016 at 12:30 

	select * from datastax.eventsource where solr_query = '{"q":"eventtype:LOGIN", "fq": "time:[2016-08-09T12:30:00.000Z TO 2016-08-11T12:30:00.000Z]", "sort":"time desc"}' limit 10000;

To use Spark, using DSE we can just 'dse spark' to use the repl.

First we will create an Event object which will hold our events objects

```
case class Event (date: String, bucket: Int, id: java.util.UUID, data: String, eventtype: String, 
aggregatetype: String, time: java.util.Date, loglevel: String, host: String); 

val events =  sc.cassandraTable[Event]("datastax", "eventsource").cache; 
events.count

val max = events.map(_.time).max
val min = events.map(_.time).min
```

We can query our data and return events before or after a certain time.

```
val yesterday = new java.util.Date(java.util.Calendar.getInstance().getTime().getTime()-200000000);
yesterday

val before = events.filter(_.time.before(yesterday)); 
before.take(10).foreach(print) 
before.count
 
val after = events.filter(_.time.after(yesterday)); 
after.take(10).foreach(print) 
after.count
```

Or we can use filtering to just get the events between two dates. 

```
val start = new java.util.Date(java.util.Calendar.getInstance().getTime().getTime()-200000000);
val end = new java.util.Date(java.util.Calendar.getInstance().getTime().getTime()-190000000);

val filtered = events.filter(_.time.after(start)).filter(_.time.before(end)).cache;
filtered.count
```

Lets get all number of events per host and a list of all distinct hosts.
```
var hostCounts =  events.map(f => (f.host, 1)).reduceByKey(_ + _)
hostCounts.collect().foreach(println)

var hosts =  hostCounts.map(f => (f._1))
hosts.collect().foreach(println)
```

To use spark sql - try the following with a valid date 
```
val results = sqlContext.sql("SELECT * from datastax.eventsource where date = '20161019'")
 
results.take(5).foreach(println)

val results = sqlContext.sql("SELECT * from datastax.eventsource where time > '2016-10-22 16:18:07' ");

results.take(5).foreach(println)

val results = sqlContext.sql("SELECT * from datastax.eventsource where time > '2016-10-22 16:18:07' and time < '2016-10-23 16:18:07'");

results.count
```

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    


    
