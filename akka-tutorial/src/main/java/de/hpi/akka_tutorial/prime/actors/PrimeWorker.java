package de.hpi.akka_tutorial.prime.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.akka_tutorial.prime.messages.NumbersMessage;
import de.hpi.akka_tutorial.prime.messages.RangeMessage;

@Deprecated
public class PrimeWorker extends AbstractActor {
	
	public static Props props() {
		return Props.create(PrimeWorker.class);
	}

	private final LoggingAdapter logger = Logging.getLogger(this.getContext().getSystem(), this);
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RangeMessage.class, this::handle)
				.matchAny(object -> this.logger.info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(RangeMessage numberRangeMessage) {
		
		// Iterate over the range of numbers, compute the primes, and return the list of numbers that are prime
		this.logger.info("Processing " + numberRangeMessage.getStartNumber() + " to " + numberRangeMessage.getEndNumber());
		NumbersMessage result = new NumbersMessage();
		for (long number = numberRangeMessage.getStartNumber(); number <= numberRangeMessage.getEndNumber(); number++)
			if (this.isPrime(number))
				result.addNumber(number);
		
		// Send the result back to the sender
		this.getSender().tell(result, getSelf());
	}

	private boolean isPrime(long n) {
		
		// Check for the most basic primes
		if (n == 1 || n == 2 || n == 3)
			return true;

		// Check if n is an even number
		if (n % 2 == 0)
			return false;

		// Check the odds
		for (long i = 3; i * i <= n; i += 2)
			if (n % i == 0)
				return false;
		
		return true;
	}
	
}
