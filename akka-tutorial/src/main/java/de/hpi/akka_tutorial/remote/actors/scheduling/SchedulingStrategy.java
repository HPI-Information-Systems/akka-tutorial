package de.hpi.akka_tutorial.remote.actors.scheduling;

import akka.actor.ActorRef;
import de.hpi.akka_tutorial.remote.actors.Worker;

public interface SchedulingStrategy {
	
	/**
	 * Schedule a new prime checking task in the given range.
	 *
	 * @param taskId the id of the task that is to be split and scheduled
	 * @param startNumber first number of the range
	 * @param endNumber last number of the range
	 */
	public void schedule(final int taskId, final long startNumber, final long endNumber);
	
	/**
	 * Notify the completion of a worker's task.
	 *
	 * @param taskId the id of the task this worker was working on
	 * @param worker the reference to the worker who finished the task
	 */
	public void finished(final int taskId, final ActorRef worker);
	
	/**
	 * Check if there are still any pending tasks.
	 *
	 * @return {@code true} if tasks are still pending
	 */
	public boolean hasTasksInProgress();
	
	/**
	 * Add a new {@link Worker} actor.
	 *
	 * @param worker the worker actor to add
	 */
	public void addWorker(final ActorRef worker);
	
	/**
	 * Remove a {@link Worker} actor.
	 *
	 * @param worker the worker actor to remove
	 */
	public void removeWorker(final ActorRef worker);
	
}
