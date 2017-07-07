package de.hpi.akka_tutorial.prime.actors;

import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import de.hpi.akka_tutorial.prime.messages.NumbersMessage;
import de.hpi.akka_tutorial.prime.messages.RangeMessage;

public class PrimeMaster extends AbstractActor {
	
	public static Props props(final int numberOfWorkers, final ActorRef listener) {
		return Props.create(PrimeMaster.class, () -> new PrimeMaster(numberOfWorkers, listener));
	}

	private final LoggingAdapter logger = Logging.getLogger(this.getContext().getSystem(), this);

	private final Router workerRouter;
	private final ActorRef primeListener;

	private final int numberOfWorkers;
	private int numberOfResults = 0;

	private NumbersMessage primeNumbers = new NumbersMessage();

	public PrimeMaster(final int numberOfWorkers, final ActorRef primeListener) {
		
		// Save our parameters locally
		this.numberOfWorkers = numberOfWorkers;
		this.primeListener = primeListener;
		
		// Create routees and router
		List<Routee> routees = new ArrayList<Routee>();
		for (int i = 0; i < numberOfWorkers; i++) {
			ActorRef workerRef = this.getContext().actorOf(PrimeWorker.props());
			this.getContext().watch(workerRef);
			routees.add(new ActorRefRoutee(workerRef));
		}
		this.workerRouter = new Router(new RoundRobinRoutingLogic(), routees);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RangeMessage.class, this::handle)
				.match(NumbersMessage.class, this::handle)
				.matchAny(object -> this.logger.info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(RangeMessage numberRangeMessage) {
		
		// Break the work up into 10 chunks of numbers
		final long numberOfNumbers = numberRangeMessage.getEndNumber() - numberRangeMessage.getStartNumber();
		final long segmentLength = numberOfNumbers / 10;

		for (int i = 0; i < this.numberOfWorkers; i++) {
			// Compute the start and end numbers for this worker
			long startNumber = numberRangeMessage.getStartNumber() + (i * segmentLength);
			long endNumber = startNumber + segmentLength - 1;

			// Handle any remainder if this is the last worker
			if (i == this.numberOfWorkers - 1)
				endNumber = numberRangeMessage.getEndNumber();

			// Send a new message to the router for this subset of numbers
			this.workerRouter.route(new RangeMessage(startNumber, endNumber), getSelf());
		}
	}

	private void handle(NumbersMessage message) {
		
		// Add the received prime numbers from the worker to the final result
		this.primeNumbers.addNumbers(message.getNumbers());
		
		if (++this.numberOfResults >= 10) {
			// Notify the primeListener
			this.primeListener.tell(this.primeNumbers, getSelf());

			// Stop our actor hierarchy
			getContext().stop(getSelf());
		}
	}
}