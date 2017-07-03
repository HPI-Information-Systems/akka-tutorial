package de.hpi.akka_tutorial.remote;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import de.hpi.akka_tutorial.remote.actors.Listener;
import de.hpi.akka_tutorial.remote.actors.Master;

public class RemoteCalculator {

	public void calculate(long startNumber, long endNumber) {
		
		// Create the ActorSystem
		ActorSystem actorSystem = ActorSystem.create("calculator");

		// Create the Listener
		final ActorRef listener = actorSystem.actorOf(Listener.props(), "listener");

		// Create the Master
		final ActorRef master = actorSystem.actorOf(Master.props(10, listener), "master");

		// Start the calculation
		master.tell(new Master.RangeMessage(startNumber, endNumber), ActorRef.noSender());
		
		// Shutdown the ActorSystem
	//	try {
	//		Await.ready(actorSystem.whenTerminated(), Duration.create(1, TimeUnit.MINUTES));
	//	} catch (TimeoutException | InterruptedException e) {
	//		e.printStackTrace();
	//	}
	//	Future<Terminated> terminated = actorSystem.terminate();
	}
}
