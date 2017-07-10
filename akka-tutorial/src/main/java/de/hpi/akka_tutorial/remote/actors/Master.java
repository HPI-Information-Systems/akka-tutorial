package de.hpi.akka_tutorial.remote.actors;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import akka.remote.RemoteScope;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.stop;

/**
 * The master receives number of ranges that it should find all primes in. This is done by delegation to slaves.
 */
public class Master extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef listener, final int numLocalWorkers) {
		return Props.create(Master.class, () -> new Master(listener, numLocalWorkers));
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

		@Override
		public String toString() {
			return String.format("%s[%,d..%,d]", this.getClass().getSimpleName(), this.startNumber, this.endNumber);
		}
	}

	/**
	 * This message tells that no further {@link RangeMessage}s will be sent. The master should terminate then as soon
	 * as all pending work is done.
	 */
	public static class ShutdownMessage implements Serializable {

		private static final long serialVersionUID = -6270485216802183364L;
	}

	/**
	 * This message is the answer to a {@link Worker.ValidationMessage subquery}.
	 */
	public static class PrimesMessage implements Serializable {

		private static final long serialVersionUID = 4862570515887001983L;

		private final int requestId;

		private final List<Long> primes;

		private final boolean isComplete;

		/**
		 * Create a new instance.
		 *
		 * @param requestId  the ID of the query that is being served
		 * @param primes     some discovered primes
		 * @param isComplete whether all primes of the current subquery have been discovered
		 */
		public PrimesMessage(final int requestId, final List<Long> primes, boolean isComplete) {
			this.requestId = requestId;
			this.primes = primes;
			this.isComplete = isComplete;
		}
	}

	/**
	 * This message states that there is a remote actor system that the master can use to delegate work to.
	 */
	public static class RemoteSystemMessage implements Serializable {

		private static final long serialVersionUID = 2786272840353304769L;

		private final Address remoteAddress;

		public RemoteSystemMessage(Address remoteAddress) {
			this.remoteAddress = remoteAddress;
		}
	}

	private static SupervisorStrategy strategy =
			new OneForOneStrategy(0, Duration.create(1, TimeUnit.SECONDS), DeciderBuilder
					.match(Exception.class, e -> stop())
					.matchAny(o -> escalate())
					.build());

	/**
	 * This class supervises the state of a range query for primes.
	 */
	private class QueryTracker {

		/**
		 * Give each worker at most this many numbers at once to check.
		 */
		private final int MAX_SUBQUERY_RANGE_SIZE = 100_000;

		/**
		 * The query that is being processed.
		 */
		private final RangeMessage rangeMessage;

		/**
		 * This ID of the query.
		 */
		private final int id;

		/**
		 * The lower bound for the next subquery.
		 */
		private long nextMinRange;

		/**
		 * Keeps track of the currently posed subqueries and which actor is processing it.
		 */
		private Map<ActorRef, Worker.ValidationMessage> runningSubqueries = new HashMap<>();

		/**
		 * Keeps track of failed subqueries, so as to reschedule them to some worker.
		 */
		private final Queue<Worker.ValidationMessage> failedSubqueries = new LinkedList<>();

		/**
		 * Collects the result.
		 */
		private final List<Long> primes = new ArrayList<>();

		private QueryTracker(int id, RangeMessage rangeMessage) {
			this.rangeMessage = rangeMessage;
			this.id = id;
			this.nextMinRange = rangeMessage.startNumber;
		}

		/**
		 * Create a new subquery to ask some worker. If a subquery was available, it will be registered as "in progress"
		 * with the worker.
		 *
		 * @return a new subquery or {@code null}
		 */
		Worker.ValidationMessage pollNextSubquery(ActorRef actorRef) {
			// Restart a failed subquery if any
			if (!this.failedSubqueries.isEmpty()) {
				Worker.ValidationMessage subquery = this.failedSubqueries.poll();
				this.runningSubqueries.put(actorRef, subquery);
				return subquery;
			}

			// Otherwise, form a new subquery
			long subqueryRangeSize = Math.min(this.rangeMessage.endNumber - this.nextMinRange + 1, MAX_SUBQUERY_RANGE_SIZE);
			if (subqueryRangeSize > 0) {
				Worker.ValidationMessage subquery = new Worker.ValidationMessage(this.id, this.nextMinRange, this.nextMinRange += subqueryRangeSize);
				this.runningSubqueries.put(actorRef, subquery);
				return subquery;
			}

			// If that's also not possible, return null
			return null;
		}

		/**
		 * Handle the failure of a running subquery. That is, prepare to re-schedule the failed subquery.
		 *
		 * @param actorRef the failing actor whose subquery is to be re-scheduled
		 */
		void handleFailure(ActorRef actorRef) {
			Worker.ValidationMessage failedTask = this.runningSubqueries.remove(actorRef);
			if (failedTask != null) {
				this.failedSubqueries.add(failedTask);
			}
		}

		/**
		 * Collect an intermediate result.
		 *
		 * @param worker that produced the result
		 * @param primes the resulting primes
		 */
		void collectResult(ActorRef worker, Collection<Long> primes, boolean isComplete) {
			if (isComplete) {
				// Mark the query as completed
				Worker.ValidationMessage finishedTask = this.runningSubqueries.remove(worker);
				assert finishedTask != null;
			}

			// Collect the results
			this.primes.addAll(primes);
		}

		/**
		 * Checks whether this query is complete, i.e., there are no more open or running subqueries.
		 *
		 * @return whether this query is complete
		 */
		boolean isComplete() {
			return this.runningSubqueries.isEmpty()
					&& this.failedSubqueries.isEmpty()
					&& this.nextMinRange > this.rangeMessage.endNumber;
		}

	}

	private final ActorRef listener;

	/**
	 * The number of workers to run locally.
	 */
	private final int numLocalWorkers;

	/**
	 * Helper variable to assign unique IDs to each range query.
	 */
	private int nextQueryId = 0;

	/**
	 * Keep track of the progress of each range query. The queries are kept in their insertion order.
	 */
	private final LinkedHashMap<Integer, QueryTracker> queryId2tracker = new LinkedHashMap<>();

	/**
	 * Keep track of what each worker is currently doing.
	 */
	private final Map<ActorRef, QueryTracker> worker2tracker = new HashMap<>();

	/**
	 * Whether this actor is still accepting new {@link RangeMessage}s.
	 */
	private boolean isAcceptingRequests = true;

	public Master(final ActorRef listener, int numLocalWorkers) {
		// Save the reference to the Listener actor
		this.listener = listener;
		this.numLocalWorkers = numLocalWorkers;
	}

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return Master.strategy;
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);

		// Start the specified number of local workers
		for (int i = 0; i < this.numLocalWorkers; i++) {
			// Create a new worker with the given URI
			ActorRef worker = this.getContext().actorOf(Worker.props());
			this.worker2tracker.put(worker, null);

			// Add the worker to the watch list and our router
			this.getContext().watch(worker);
		}
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();
		log().info("Stopping {}...", self());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RemoteSystemMessage.class, this::handle)
				.match(RangeMessage.class, this::handle)
				.match(PrimesMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(RemoteSystemMessage message) {

		// Create a new worker with the given URI
		ActorRef worker = this.getContext().actorOf(Worker.props().withDeploy(new Deploy(new RemoteScope(message.remoteAddress))));
		this.worker2tracker.put(worker, null);

		// Add the worker to the watch list and our router
		this.getContext().watch(worker);

		this.log().info("New worker: " + worker);

		// Assign possibly open subqueries to the new worker
		this.assignSubqueries();
	}

	private void handle(RangeMessage message) {
		if (!this.isAcceptingRequests) {
			this.log().warning("Discarding request {}.", message);
			return;
		}

		// Create a new tracker for the query
		QueryTracker tracker = new QueryTracker(this.nextQueryId++, message);
		this.queryId2tracker.put(tracker.id, tracker);

		// Assign existing, possible free, workers to the new query
		this.assignSubqueries();
	}

	/**
	 * Assign currently free workers to subqueries.
	 */
	private void assignSubqueries() {
		Collection<ActorRef> freeWorkers = this.worker2tracker.entrySet().stream()
				.filter(e -> e.getValue() == null)
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());

		Iterator<QueryTracker> queryTrackerIterator = this.queryId2tracker.values().iterator();
		if (!queryTrackerIterator.hasNext()) 
			return;
		QueryTracker nextQueryTracker = queryTrackerIterator.next();

		for (ActorRef freeWorker : freeWorkers) {
			// Determine the next open subquery
			Worker.ValidationMessage subquery;
			while ((subquery = nextQueryTracker.pollNextSubquery(freeWorker)) == null) {
				if (!queryTrackerIterator.hasNext()) 
					return;
				nextQueryTracker = queryTrackerIterator.next();
			}

			// Assign the subquery to the worker and keep track of the assignment
			freeWorker.tell(subquery, this.getSelf());
			this.worker2tracker.put(freeWorker, nextQueryTracker);
		}

	}

	/**
	 * Stop receiving new queries.
	 */
	private void handle(ShutdownMessage message) {
		this.isAcceptingRequests = false;
		if (!this.hasOpenQueries()) {
			this.stopSelfAndListener();
		}
	}

	/**
	 * Tells if there are still any pending responses.
	 *
	 * @return if there are still any pending responses
	 */
	private boolean hasOpenQueries() {
		return !this.queryId2tracker.isEmpty();
	}

	/**
	 * Receive a result of a subquery, i.e., a bunch of primes.
	 *
	 * @param message the primes
	 */
	private void handle(PrimesMessage message) {
		// Find out who returned the result
		final ActorRef worker = this.getSender();

		// Find the query being processed
		QueryTracker queryTracker = this.queryId2tracker.get(message.requestId);
		queryTracker.collectResult(worker, message.primes, message.isComplete);

		// Log a short status update
		this.log().info(String.format("Got %,d primes between %,d and %,d.", queryTracker.primes.size(), queryTracker.rangeMessage.startNumber, queryTracker.rangeMessage.endNumber));

		// If the worker only returned an intermediate result, no further action is required
		if (!message.isComplete) 
			return;

		// Mark the worker as free
		this.worker2tracker.put(worker, null);

		// Check if the query is complete
		if (queryTracker.isComplete()) {
			String primeList = queryTracker.primes.stream()
					.mapToLong(Long::longValue)
					.sorted()
					.mapToObj(prime -> String.format("%,d", prime))
					.collect(Collectors.joining("; "));
			String queryResult = String.format("Primes between %,d and %,d: %s", queryTracker.rangeMessage.startNumber, queryTracker.rangeMessage.endNumber, primeList);

			// Notify the listener
			this.listener.tell(new Listener.PrintMessage(queryResult), this.getSelf());

			// Remove the query tracker
			this.queryId2tracker.remove(queryTracker.id);

			// Check if work is now complete
			if (!this.isAcceptingRequests && !this.hasOpenQueries()) {
				this.stopSelfAndListener();
			}
		} else {
			// Re-assign the now free worker
			this.assignSubqueries();
		}
	}

	/**
	 * Handle a terminated worker that might have been running a query.
	 */
	private void handle(Terminated message) {
		// Find out who terminated
		final ActorRef terminatedActor = this.getSender();

		// Remove it from the list of worker
		QueryTracker processedTracker = this.worker2tracker.remove(terminatedActor);

		// If the worker was processing some subquery, then we need to re-schedule this subquery
		if (processedTracker != null) {
			this.log().warning("{} has terminated while processing {}.", terminatedActor, processedTracker.rangeMessage);
			processedTracker.handleFailure(terminatedActor);

			// We might have some free workers that could process the re-scheduled subquery
			this.assignSubqueries();
		}

		// TODO: Why do we initiate a stop when a worker terminated? Since we do not stop the shepherd here, the actor system would not terminate?
		this.isAcceptingRequests = false;
		if (!this.hasOpenQueries()) {
			this.stopSelfAndListener();
		}
	}

	/**
	 * Stop this master and its associated listener.
	 */
	private void stopSelfAndListener() {
		this.log().info("Stopping...");
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
		this.listener.tell(PoisonPill.getInstance(), this.getSelf());
	}
}