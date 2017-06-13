package com.datastax.events;

import java.util.concurrent.BlockingQueue;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.events.model.Event;
import com.datastax.events.service.EventService;

class EventWriter implements KillableRunner {

	private volatile boolean shutdown = false;
	private EventService service;
	private BlockingQueue<Event> queue;

	public EventWriter(EventService service, BlockingQueue<Event> queue) {
		this.service = service;
		this.queue = queue;
	}

	@Override
	public void run() {
		Event event;
		while(!shutdown){				
			event = queue.poll(); 
			
			if (event!=null){
				try {
					this.service.insertEvent(event);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}				
		}				
	}
	
	@Override
    public void shutdown() {
		while(!queue.isEmpty())
			
		shutdown = true;
    }
}
