package com.datastax.events;

import java.text.ParseException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.events.model.Event;
import com.datastax.events.service.EventService;
import com.datastax.events.utils.DateUtils;

public class ReadEvents {
	
	private static Logger logger = LoggerFactory.getLogger(ReadEvents.class);

	public ReadEvents() {

		BlockingQueue<Event> queue = new ArrayBlockingQueue<Event>(100);
		
		//Executor for Threads
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "8"));
		
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		EventService service = new EventService();
		EventReader eventReader = new EventReader(queue);
		executor.execute(eventReader);
		
		DateTime from = DateTime.now();
		DateTime to = DateTime.now();
		
		try {
			from = DateUtils.parseDate(PropertyHelper.getProperty("from","20160205-000000"));
			to = DateUtils.parseDate(PropertyHelper.getProperty("to","20160206-000000"));
		} catch (ParseException e) {
			String error = "Caught exception parsing dates " + from + "-" + to;			
			logger.error(error);
		}
		
		Timer timer = new Timer();
		service.getEvents(queue, from, to, null);		
		timer.end();			
		
		long count = eventReader.getCount();
		
		logger.info("Time taken to read " + count + " events was " + timer.getTimeTakenSeconds() + " secs");
		
		timer = new Timer();
		
		//Only get eventType PREFERENCES
		String eventType = "PREFERENCES";
		service.getEvents(queue, from, to, eventType);
		
		timer.end();			
		long count1 = eventReader.getCount() - count;
		logger.info("Time taken to read " + count1 + " " + eventType + " events was " + timer.getTimeTakenSeconds() + " secs");
		
		System.exit(0);	
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new ReadEvents();

		System.exit(0);
	}

}
