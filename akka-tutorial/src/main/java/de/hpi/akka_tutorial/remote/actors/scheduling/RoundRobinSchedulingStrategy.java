package de.hpi.akka_tutorial.remote.actors.scheduling;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Router;
import de.hpi.akka_tutorial.remote.actors.Worker;

public class RoundRobinSchedulingStrategy implements SchedulingStrategy {

	/**
	 * {@link SchedulingStrategy.Factory} implementation for the {@link RoundRobinSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategy.Factory {

		@Override
		public SchedulingStrategy create(ActorRef master) {
			return new RoundRobinSchedulingStrategy(master);
		}
	}

	// A round robin router for our workers
	private Router workerRouter = new Router(new RoundRobinRoutingLogic());

	// The number of workers currently available for scheduling
	private int numberOfWorkers = 0;
	
	// A map of pending responses for unfinished tasks
	private Map<Integer, Integer> taskId2numberPendingResponses = new HashMap<>();
	
	// A reference to the actor in whose name we send messages
	private final ActorRef master;

	public RoundRobinSchedulingStrategy(ActorRef master) {
		this.master = master;
	}
	
	@Override
	public void schedule(final int taskId, final long startNumber, final long endNumber) {
		
		// Break the work up into numberOfWorkers chunks of numbers
		final long numberOfNumbers = endNumber - startNumber + 1;
		final long segmentLength = numberOfNumbers / this.numberOfWorkers;

		for (int i = 0; i < this.numberOfWorkers; i++) {
			
			// Compute the start and end numbers for this worker
			long currentStartNumber = startNumber + (i * segmentLength);
			long currentEndNumber = currentStartNumber + segmentLength - 1;

			// Handle any remainder if this is the last worker
			if (i == this.numberOfWorkers - 1)
				currentEndNumber = endNumber;

			// Send a new message to the router for this subset of numbers
			this.workerRouter.route(new Worker.ValidationMessage(taskId, currentStartNumber, currentEndNumber), this.master);
		}
		
		// Store the task with numberOfWorkers pending responses
		this.taskId2numberPendingResponses.put(taskId, this.numberOfWorkers);
	}

	@Override
	public void finished(final int taskId, final ActorRef worker) {
		
		// Decrement the number of pending responses for this task
		final int newPendingResponses = this.taskId2numberPendingResponses.get(taskId) - 1;
		
		if (newPendingResponses == 0) {
			// Task is completed
			this.taskId2numberPendingResponses.remove(taskId);
		} else {
			// Task is still pending
			this.taskId2numberPendingResponses.put(taskId, newPendingResponses);
		}
	}

	@Override
	public boolean hasTasksInProgress() {
		return !this.taskId2numberPendingResponses.isEmpty();
	}

	@Override
	public void addWorker(final ActorRef worker) {
		
		// Increment the worker count
		this.numberOfWorkers++;
		
		// Add the worker to the router
		this.workerRouter = this.workerRouter.addRoutee(worker);
	}

	@Override
	public void removeWorker(final ActorRef worker) {
		
		// Decrement the worker count
		this.numberOfWorkers--;
		
		// Remove the worker from the router
		this.workerRouter = this.workerRouter.removeRoutee(worker);
	}

	@Override
	public int countWorkers() {
		return this.numberOfWorkers;
	}
}
