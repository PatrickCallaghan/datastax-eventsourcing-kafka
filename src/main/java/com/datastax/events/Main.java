package com.datastax.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.ThreadUtils;
import com.datastax.demo.utils.Timer;
import com.datastax.events.data.EventGenerator;
import com.datastax.events.model.Event;
import com.datastax.events.service.EventService;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {

		String noOfEventsStr = PropertyHelper.getProperty("noOfEvents", "100000");
		int noOfDays = Integer.parseInt(PropertyHelper.getProperty("noOfDays", "32"));

		BlockingQueue<Event> queue = new ArrayBlockingQueue<Event>(100);
		List<KillableRunner> tasks = new ArrayList<>();
		
		//Executor for Threads
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "4"));
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		EventService service = new EventService();
		
		int noOfEvents = Integer.parseInt(noOfEventsStr);
		
		int totalEvents = noOfEvents*noOfDays;
		logger.info("Writing " + totalEvents + " historic events");

		for (int i = 0; i < noOfThreads; i++) {
			
			KillableRunner task = new EventWriter(service, queue);
			executor.execute(task);
			tasks.add(task);
		}					
		
		Timer timer = new Timer();
		for (int i = 0; i < totalEvents; i++) {
			
			try{
				queue.put(EventGenerator.createRandomEvent(noOfEvents, noOfDays));
				
				if (EventGenerator.eventCounter.get() % 100000 == 0){
					sleep(1000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
		timer.end();
		
		logger.info("Writing realtime events");
		
		Random r = new Random();
		 
		while(true){
			try{
				queue.put(EventGenerator.createRandomEventNow());
				
				sleep(new Double(Math.random()*15).intValue());
				
				double d = r.nextGaussian()*-1d;
				//Create an random event
				
				int test = new Double(Math.random()*10).intValue();
				
				if (d*test < 1){
					logger.info("Creating random events");
					int someNumber = new Double(Math.random()*10).intValue();
					
					for (int i=0; i < someNumber; i++){
						queue.put(EventGenerator.createRandomEventNow());
						
						
						if (new Double(Math.random()*100).intValue() > 2){
							sleep(new Double(Math.random()*100).intValue());
						}
					}
				}
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
		ThreadUtils.shutdown(tasks, executor);
			
		System.exit(0);
	}

	private void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

}
