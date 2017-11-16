package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;

/**
 * The listener collects prime numbers and responds to action requests on these primes.
 */
public class Listener extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "listener";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link Listener} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(Listener.class);
	}

	/**
	 * Asks the {@link Listener} to store a given set of primes.
	 */
	public static class PrimesMessage implements Serializable {
		
		private static final long serialVersionUID = -1779142448823490939L;
		
		private List<Long> primes;
		
		/**
		 * Construct a new {@link PrimesMessage} object.
		 * 
		 * @param primes A list of prime numbers
		 */
		public PrimesMessage(final List<Long> primes) {
			this.primes = primes;
		}

		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private PrimesMessage() {
		}
	}
	
	/**
	 * Asks the {@link Listener} to log all its primes.
	 */
	public static class LogPrimesMessage implements Serializable {
		
		private static final long serialVersionUID = -5646268930296638375L;
	}

	/**
	 * Asks the {@link Listener} to log its largest prime.
	 */
	public static class LogMaxMessage implements Serializable {

		private static final long serialVersionUID = 9210465485942285762L;
	}

	// The set of all prime numbers received by this listener actor
	private final Set<Long> primes = new HashSet<>();
	
	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}


	@Override
	public void postStop() throws Exception {
		super.postStop();
		
		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PrimesMessage.class, this::handle)
				.match(LogPrimesMessage.class, this::handle)
				.match(LogMaxMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(PrimesMessage message) {
		this.primes.addAll(message.primes);
	}
	
	private void handle(LogPrimesMessage message) {
		String primeList = this.primes.stream()
				.mapToLong(Long::longValue)
				.sorted()
				.mapToObj(prime -> String.valueOf(prime))
				.collect(Collectors.joining(";"));
		this.log().info(String.format("Primes: %s", primeList));		
	}

	private void handle(LogMaxMessage message) {
		Long prime = this.primes.stream()
				.collect(Collectors.maxBy(Comparator.naturalOrder()))
				.orElseGet(() -> { return 0L; });
		this.log().info(String.format("Max prime: %d", prime));		
	}

	private void handle(ShutdownMessage message) {
		// We could write all primes to disk here
		
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}
	
}
