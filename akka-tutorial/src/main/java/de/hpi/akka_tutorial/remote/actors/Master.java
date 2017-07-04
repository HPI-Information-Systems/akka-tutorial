package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.AddressFromURIString;
import akka.actor.Deploy;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.remote.RemoteScope;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import de.hpi.akka_tutorial.remote.actors.Shepherd.Subscription;
import scala.concurrent.duration.Duration;
import static akka.actor.SupervisorStrategy.*;

public class Master extends AbstractLoggingActor {
	
	public static Props props(final List<Subscription> subscriptions, ActorRef listener) {
		return Props.create(Master.class, () -> new Master(subscriptions, listener));
	}
	
	private static SupervisorStrategy strategy =
			new OneForOneStrategy(0, Duration.create(1, TimeUnit.SECONDS), DeciderBuilder
					.match(Exception.class, e -> escalate())
					.matchAny(o -> escalate())
					.build());

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

	public Master(final List<Subscription> subscriptions, ActorRef listener) {
		
		// Save our parameters locally
		this.numberOfWorkers = subscriptions.size();
		this.listener = listener;
		
		// Create routees and router
		List<Routee> routees = new ArrayList<Routee>();
		for (Subscription subscription : subscriptions) {
			
			Address address = AddressFromURIString.parse(subscription.toString());
			
			ActorRef worker = this.getContext().actorOf(Worker.props().withDeploy(new Deploy(new RemoteScope(address))));
			
			this.getContext().watch(worker);
			
			routees.add(new ActorRefRoutee(worker));
		}
		this.workerRouter = new Router(new RoundRobinRoutingLogic(), routees);
	}

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return Master.strategy;
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
		
		if (++this.numberOfResults >= this.numberOfWorkers) {
			// Notify the listener
			this.listener.tell(new Listener.StringsMessage(this.strings), getSelf());

			// Stop our actor hierarchy
			getContext().stop(getSelf());
		}
	}
}