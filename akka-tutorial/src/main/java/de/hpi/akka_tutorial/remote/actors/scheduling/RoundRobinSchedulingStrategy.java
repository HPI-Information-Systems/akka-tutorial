package de.hpi.akka_tutorial.remote.actors.scheduling;

import akka.actor.ActorRef;

public class RoundRobinSchedulingStrategy implements SchedulingStrategy {

	@Override
	public void schedule(int taskId, long startNumber, long endNumber) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void finished(int taskId, ActorRef worker) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean hasTasksInProgress() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addWorker(ActorRef worker) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeWorker(ActorRef worker) {
		// TODO Auto-generated method stub
		
	}

}
