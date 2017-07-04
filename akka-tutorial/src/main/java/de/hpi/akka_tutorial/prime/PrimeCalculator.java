package de.hpi.akka_tutorial.prime;

import java.util.concurrent.TimeoutException;

//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
//import scala.concurrent.Await;
//import scala.concurrent.Future;
//import scala.concurrent.duration.Duration;
import de.hpi.akka_tutorial.prime.actors.PrimeListener;
import de.hpi.akka_tutorial.prime.actors.PrimeMaster;
import de.hpi.akka_tutorial.prime.messages.RangeMessage;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class PrimeCalculator {
	
	public void calculate(long startNumber, long endNumber) {
		
		// Create the ActorSystem
		final ActorSystem actorSystem = ActorSystem.create("primeCalculator");

		// Create the PrimeListener
		final ActorRef primeListener = actorSystem.actorOf(PrimeListener.props(), "primeListener");

		// Create the PrimeMaster
		final ActorRef primeMaster = actorSystem.actorOf(PrimeMaster.props(10, primeListener), "primeMaster");

		// Start the calculation
		primeMaster.tell(new RangeMessage(startNumber, endNumber), ActorRef.noSender());
		
		// Shutdown the ActorSystem
		try {
			Await.ready(actorSystem.whenTerminated(), Duration.Inf());
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("ActorSystem finished!");
		
	//	Future<Terminated> terminated = actorSystem.terminate();
	}

}