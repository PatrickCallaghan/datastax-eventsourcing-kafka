package com.datastax.events.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.events.dao.EventDao;
import com.datastax.events.model.Event;

public class EventService {

	private static final String EVENTSOURCE = "eventsource";
	private static Logger logger = LoggerFactory.getLogger(EventService.class);
	private EventDao dao;
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	private KafkaProducer<String, String> producer;
	private AtomicLong counter = new AtomicLong(0);
	private Future<RecordMetadata> send;
	private RecordMetadata recordMetadata;

	public EventService() {
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new EventDao(contactPointsStr.split(","));

		// Set up Kafka producer
		String bootstrapServer = PropertyHelper.getProperty("bootstrapServer", "localhost:9092");

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServer);
		props.put("metadata.fetch.timeout.ms",1000);
		props.put("request.timeout.ms",25);
		props.put("acks", "all");
		props.put("retries", 1);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
	}

	public void getEvents(BlockingQueue<Event> queue, DateTime from, DateTime to, String eventType) {

		// Get all minutes between from and to dates
		DateTime time = from;

		while (time.isBefore(to)) {
			dao.getEventsForDate(queue, time, eventType);
			time = time.plusMinutes(1);
		}
	}

	public List<Event> getEvents(DateTime from, DateTime to) {
		return this.getEvents(from, to, null);
	}

	public List<Event> getEvents(DateTime from, DateTime to, String eventType) {

		final List<Event> events = new ArrayList<Event>();
		final BlockingQueue<Event> queue = new ArrayBlockingQueue<Event>(10000);

		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				while (true) {
					Event event = queue.poll();
					if (event != null) {
						events.add(event);
					}
				}
			}
		};

		executor.execute(runnable);

		// Get all minutes between from and to dates
		DateTime time = from;
		while (time.isBefore(to)) {
			dao.getEventsForDate(queue, time, eventType);

			time = time.plusMinutes(1);
		}

		return events;
	}

	public void insertEventSync(Event event) {
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(EVENTSOURCE, "" + counter.incrementAndGet(), event.toString());
		
		dao.insertEvent(event);
		
		try {
			recordMetadata = producer.send(record).get();
		
		}catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			logger.info(e.getMessage());
			logger.info(record.key() + " - " + record.value());			
		}		
	}

	public void insertEventAsync(Event event) {
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(EVENTSOURCE, "" + counter.incrementAndGet(), event.toString());
		
		dao.insertEvent(event);
		producer.send(record, new CallBack(record));
	}
	
	class CallBack implements Callback{

		
		private ProducerRecord<String, String> record;

		public CallBack(ProducerRecord<String, String> record){
			this.record = record;
		}
				
		@Override
		public void onCompletion(RecordMetadata rm, Exception e) {
			if (e != null){
				logger.info(e.getMessage());
				logger.info(record.key() + " - " + record.value());			
			}
		}
	}
	
	@Override
	public void finalize() {
		producer.close();
		dao.close();
	}
}
