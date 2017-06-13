package com.datastax.events.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.events.dao.EventDao;
import com.datastax.events.model.Event;

public class EventService {

	private EventDao dao;
	private ExecutorService executor = Executors.newFixedThreadPool(4);
	private KafkaProducer<String, String> producer;
	private AtomicLong counter = new AtomicLong(0);

	public EventService() {
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new EventDao(contactPointsStr.split(","));

		// Set up Kafka producer
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
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

	public void insertEvent(Event event) {
		
		dao.insertEvent(event);
		producer.send(
				new ProducerRecord<String, String>("eventsource", "" + counter.incrementAndGet(), event.toString()));
	}

	@Override
	public void finalize() {
		producer.close();
		dao.close();
	}
}
