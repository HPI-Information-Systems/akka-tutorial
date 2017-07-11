package de.hpi.akka_tutorial.remote.actors.scheduling;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import akka.actor.ActorRef;
import de.hpi.akka_tutorial.remote.actors.Worker;

public class LoadAwareSchedulingStrategy implements SchedulingStrategy {
	
	/**
	 * This class supervises the state of a range query for primes.
	 */
	private class QueryTracker {

		// Give each worker at most this many numbers at once to check.
		private final int MAX_SUBQUERY_RANGE_SIZE = 100_000;

		// The range of values that was not yet scheduled to workers.
		private long remainingRangeStartNumber, remainingRangeEndNumber;

		// This is the ID of the query that is being tracked.
		private final int id;
		
		// Keeps track of the currently posed subqueries and which actor is processing it.
		private final Map<ActorRef, Worker.ValidationMessage> runningSubqueries = new HashMap<>();

		// Keeps track of failed subqueries, so as to reschedule them to some worker.
		private final Queue<Worker.ValidationMessage> failedSubqueries = new LinkedList<>();
		
		private QueryTracker(final int id, final long startNumber, final long endNumber) {
			this.id = id;
			this.remainingRangeStartNumber = startNumber;
			this.remainingRangeEndNumber = endNumber;
		}

		/**
		 * Create a new subquery to ask some worker. If a subquery was available, it will be registered as "in progress" with the worker.
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
			long subqueryRangeSize = Math.min(this.remainingRangeEndNumber - this.remainingRangeStartNumber + 1, MAX_SUBQUERY_RANGE_SIZE);
			if (subqueryRangeSize > 0) {
				Worker.ValidationMessage subquery = new Worker.ValidationMessage(this.id, this.remainingRangeStartNumber, this.remainingRangeStartNumber + subqueryRangeSize - 1);
				this.remainingRangeStartNumber += subqueryRangeSize;
				this.runningSubqueries.put(actorRef, subquery);
				return subquery;
			}

			// Otherwise, return null as there is nothing to do at the moment
			return null;
		}

		/**
		 * Handle the failure of a subquery. That is, prepare to re-schedule the failed subquery.
		 *
		 * @param worker the actor that just failed
		 */
		void workFailed(ActorRef worker) {
			Worker.ValidationMessage failedTask = this.runningSubqueries.remove(worker);
			if (failedTask != null) {
				this.failedSubqueries.add(failedTask);
			}
		}

		/**
		 * Handle the completion of a subquery.
		 *
		 * @param worker the actor that just completed
		 */
		void workCompleted(ActorRef worker) {
			Worker.ValidationMessage completedTask = this.runningSubqueries.remove(worker);
			assert completedTask != null;
		}

		/**
		 * Check whether this query is complete, i.e., there are no more open or running subqueries.
		 *
		 * @return whether this query is complete
		 */
		boolean isComplete() {
			return this.runningSubqueries.isEmpty()
					&& this.failedSubqueries.isEmpty()
					&& this.remainingRangeStartNumber > this.remainingRangeEndNumber;
		}
	}
	

	// A mapping of pending range queries to the query tracker that watches the progress of each range query; the queries are kept in their insertion order
	private final LinkedHashMap<Integer, QueryTracker> queryId2tracker = new LinkedHashMap<>();

	// A mapping of known works to their current task
	private final Map<ActorRef, QueryTracker> worker2tracker = new HashMap<>();

	// A reference to the actor in whose name we send messages
	private final ActorRef master;
	
	public LoadAwareSchedulingStrategy(ActorRef master) {
		this.master = master;
	}

	@Override
	public void schedule(final int taskId, final long startNumber, final long endNumber) {
		
		// Create a new tracker for the query
		QueryTracker tracker = new QueryTracker(taskId, startNumber, endNumber);
		this.queryId2tracker.put(tracker.id, tracker);

		// Assign existing, possible free, workers to the new query
		this.assignSubqueries();		
	}
	
	@Override
	public boolean hasTasksInProgress() {
		return !this.queryId2tracker.isEmpty();
	}
	
	@Override
	public void finished(int taskId, ActorRef worker) {
		
		// Find the query being processed
		QueryTracker queryTracker = this.queryId2tracker.get(taskId);
		
		// Mark the worker as free
		queryTracker.workCompleted(worker);
		this.worker2tracker.put(worker, null);

		// Check if the query is complete
		if (queryTracker.isComplete()) {
			// Remove the query tracker
			this.queryId2tracker.remove(queryTracker.id);
		} else {
			// Re-assign the now free worker
			this.assignSubqueries();
		}
	}
	
	@Override
	public void addWorker(final ActorRef worker) {
		
		// Add the new worker
		this.worker2tracker.put(worker, null);
		
		// Assign possibly open subqueries to the new worker
		this.assignSubqueries();
	}

	@Override
	public void removeWorker(final ActorRef worker) {
		
		// Remove the worker from the list of workers
		QueryTracker processedTracker = this.worker2tracker.remove(worker);

		// If the worker was processing some subquery, then we need to re-schedule this subquery
		if (processedTracker != null) {
			processedTracker.workFailed(worker);

			// We might have some free workers that could process the re-scheduled subquery
			this.assignSubqueries();
		}
	}

	// Assign currently free workers to subqueries
	private void assignSubqueries() {
		
		// Collect all currently idle workers
		Collection<ActorRef> idleWorkers = this.worker2tracker.entrySet().stream()
				.filter(e -> e.getValue() == null)
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());

		// Try to assign new subqueries to all idle workers
		Iterator<QueryTracker> queryTrackerIterator = this.queryId2tracker.values().iterator();
		for (ActorRef idleWorker : idleWorkers) {
			while (true) {
				// Return if no work is to be scheduled
				if (!queryTrackerIterator.hasNext()) 
					return;
				
				// Retrieve the next query tracker (older queries are picked first)
				QueryTracker nextQueryTracker = queryTrackerIterator.next();
				
				// Poll the next open subquery from the query tracker; continue if no work is to be done
				Worker.ValidationMessage subquery;
				if ((subquery = nextQueryTracker.pollNextSubquery(idleWorker)) == null) {
					continue;
				}
				
				// Assign the subquery to the worker and keep track of the assignment
				idleWorker.tell(subquery, this.master);
				this.worker2tracker.put(idleWorker, nextQueryTracker);
			}
		}
	}
}
