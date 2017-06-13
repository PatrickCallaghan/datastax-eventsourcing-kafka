package com.datastax.events;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.events.model.Event;

class EventReader implements KillableRunner {

	private volatile boolean shutdown = false;
	private BlockingQueue<Event> queue;
	private AtomicLong counter = new AtomicLong(0);

	public EventReader(BlockingQueue<Event> queue) {
		this.queue = queue;
	}

	@Override
	public void run() {
		Event event;
		while (!shutdown) {
			event = queue.poll();
			
			if (event!=null){
				counter.incrementAndGet();
			}
		}
	}

	@Override
	public void shutdown() {
		shutdown = true;
	}

	public long getCount() {
		return counter.get();
		
	}
}