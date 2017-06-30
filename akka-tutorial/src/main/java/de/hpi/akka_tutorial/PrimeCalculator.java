package de.hpi.akka_tutorial;

//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
//import akka.actor.Terminated;
import de.hpi.akka_tutorial.actors.PrimeListener;
import de.hpi.akka_tutorial.actors.PrimeMaster;
import de.hpi.akka_tutorial.messages.RangeMessage;
//import scala.concurrent.Await;
//import scala.concurrent.Future;
//import scala.concurrent.duration.Duration;

public class PrimeCalculator {
	
	public void calculate(long startNumber, long endNumber) {
		// Create the ActorSystem
		ActorSystem actorSystem = ActorSystem.create("primeCalculator");

		// Create the PrimeListener
		final ActorRef primeListener = actorSystem.actorOf(PrimeListener.props(), "primeListener");

		// Create the PrimeMaster
		final ActorRef primeMaster = actorSystem.actorOf(PrimeMaster.props(10, primeListener), "primeMaster");

		// Start the calculation
		primeMaster.tell(new RangeMessage(startNumber, endNumber), ActorRef.noSender());
		
		// Shutdown the ActorSystem
	//	try {
	//		Await.ready(actorSystem.whenTerminated(), Duration.create(1, TimeUnit.MINUTES));
	//	} catch (TimeoutException | InterruptedException e) {
	//		e.printStackTrace();
	//	}
	//	Future<Terminated> terminated = actorSystem.terminate();
	}

}