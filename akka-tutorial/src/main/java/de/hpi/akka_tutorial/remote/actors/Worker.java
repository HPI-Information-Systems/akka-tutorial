package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;

public class Worker extends AbstractLoggingActor {
	
	public static Props props() {
		return Props.create(Worker.class);
	}
	
	public static class NumbersMessage implements Serializable {
		
		private static final long serialVersionUID = -7467053227355130231L;
		
		private final List<Long> numbers;

		public List<Long> getNumbers() {
			return this.numbers;
		}

		public NumbersMessage(final List<Long> numbers) {
			this.numbers = numbers;
		}
	}
	
	@Override
	public void postStop() throws Exception {
		this.context().system().terminate();
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(NumbersMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(NumbersMessage message) {
		
		this.log().info("Processing " + message.getNumbers().get(0) + " to " + message.getNumbers().get(message.getNumbers().size() - 1));

		// Do something interesting //
		
		// Iterate over the range of numbers, compute the primes, and return the list of numbers that are prime
		final List<Object> result = new ArrayList<>();
		for (Long number : message.getNumbers())
			if (this.isPrime(number.longValue()))
				result.add(number);
		
		this.getSender().tell(new Master.ObjectMessage(result), getSelf());
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