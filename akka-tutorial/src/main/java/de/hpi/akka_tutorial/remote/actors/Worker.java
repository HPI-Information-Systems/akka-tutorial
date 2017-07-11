package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;

public class Worker extends AbstractLoggingActor {

	private static final int MAX_PRIMES_PER_MESSAGE = 1000;

	/**
	 * Create the {@link Props} necessary to instantiate new {@link Worker} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(Worker.class);
	}

	/**
	 * Asks the {@link Worker} to discover all primes in a given range.
	 */
	public static class ValidationMessage implements Serializable {
		
		private static final long serialVersionUID = -7467053227355130231L;
		
		private final int id;

		/**
		 * The lower (inclusive) and upper (exclusive) range bounds to discover primes in.
		 */
		private final long rangeMin, rangeMax;

		public ValidationMessage(int id, long rangeMin, long rangeMax) {
			this.id = id;
			this.rangeMin = rangeMin;
			this.rangeMax = rangeMax;
		}
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();
		log().info("Stopping {}...", self());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ValidationMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(ValidationMessage message) {
		
		this.log().info("Discovering primes p ({} <= p <= {})...", message.rangeMin, message.rangeMax);

		// Iterate over the range of numbers, compute the primes, and return the list of numbers that are prime
		List<Long> primeBuffer = new ArrayList<>(MAX_PRIMES_PER_MESSAGE);
		for (long i = message.rangeMin; i < message.rangeMax; i++) {
			if (isPrime(i)) {
				// We must not send too large messages, hence, also reply with intermediate results as necessary.
				if (primeBuffer.size() >= MAX_PRIMES_PER_MESSAGE) {
					// Attention: Never send mutable objects in a message!!! Here, we create a copy of our mutable buffer.
					ArrayList<Long> primeBufferCopy = new ArrayList<>(primeBuffer);
					this.getSender().tell(new Master.PrimesMessage(message.id, primeBufferCopy, false), this.getSelf());
					primeBuffer.clear();
				}
				primeBuffer.add(i);
			}
		}

		// Send the primes back.
		this.getSender().tell(new Master.PrimesMessage(message.id, primeBuffer, true), this.getSelf());

		// Asynchronous version: Consider using a dedicated executor service.
//		ActorRef sender = this.getSender();
//		ActorRef self = this.getSelf();
//		getContext().getSystem().dispatcher().execute(() -> {
//			final List<Object> result = new ArrayList<>();
//			for (Long number : message.getNumbers())
//				if (this.isPrime(number.longValue()))
//					result.add(number);
//
//			sender.tell(new Master.ObjectMessage(message.getId(), result), self);
//		});
	}

	private static boolean isPrime(long n) {
		
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