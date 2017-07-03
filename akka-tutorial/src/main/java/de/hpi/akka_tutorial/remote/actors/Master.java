package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

public class Master extends AbstractLoggingActor {
	
	public static Props props(final int numberOfWorkers, ActorRef listener) {
		return Props.create(Master.class, () -> new Master(numberOfWorkers, listener));
	}

	public static class RangeMessage implements Serializable {
		
		private static final long serialVersionUID = 1538940836039448197L;

		private final long startNumber;
		private final long endNumber;
		
		public long getStartNumber() {
			return this.startNumber;
		}

		public long getEndNumber() {
			return this.endNumber;
		}

		public RangeMessage(final long startNumber, final long endNumber) {
			
			this.startNumber = startNumber;
			this.endNumber = endNumber;
		}
	}
	
	public static class ObjectMessage implements Serializable {
		
		private static final long serialVersionUID = 4862570515887001983L;
		
		private final List<Object> objects;

		public List<Object> getObjects() {
			return this.objects;
		}

		public ObjectMessage(List<Object> objects) {
			this.objects = objects;
		}

	}
	
	private final Router workerRouter;
	private final ActorRef listener;

	private final int numberOfWorkers;
	private int numberOfResults = 0;

	private final List<String> strings = new ArrayList<String>();

	public Master(final int numberOfWorkers, ActorRef listener) {
		
		// Save our parameters locally
		this.numberOfWorkers = numberOfWorkers;
		this.listener = listener;
		
		// Create routees and router
		List<Routee> routees = new ArrayList<Routee>();
		for (int i = 0; i < numberOfWorkers; i++) {
			ActorRef workerRef = this.getContext().actorOf(Worker.props());
			this.getContext().watch(workerRef);
			routees.add(new ActorRefRoutee(workerRef));
		}
		this.workerRouter = new Router(new RoundRobinRoutingLogic(), routees);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RangeMessage.class, this::handle)
				.match(ObjectMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(RangeMessage message) {
		
		// Break the work up into 10 chunks of numbers
		final long numberOfNumbers = message.getEndNumber() - message.getStartNumber();
		final long segmentLength = numberOfNumbers / 10;

		for (int i = 0; i < this.numberOfWorkers; i++) {
			// Compute the start and end numbers for this worker
			long startNumber = message.getStartNumber() + (i * segmentLength);
			long endNumber = startNumber + segmentLength - 1;

			// Handle any remainder if this is the last worker
			if (i == this.numberOfWorkers - 1)
				endNumber = message.getEndNumber();

			// Calculate the numbers
			List<Long> numbers = new ArrayList<Long>((int)(endNumber - startNumber) + 1);
			for (long number = startNumber; number <= endNumber; number++)
				numbers.add(number);
			
			// Send a new message to the router for this subset of numbers
			this.workerRouter.route(new Worker.NumbersMessage(numbers), getSelf());
		}
	}

	private void handle(ObjectMessage message) {
		
		// Add the received objects from the worker to the final result
		for (Object object : message.getObjects())
			this.strings.add(object.toString());
		
		if (++this.numberOfResults >= 10) {
			// Notify the listener
			this.listener.tell(new Listener.StringsMessage(this.strings), getSelf());

			// Stop our actor hierarchy
			getContext().stop(getSelf());
		}
	}
}