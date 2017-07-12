package de.hpi.akka_tutorial.remote;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.PoisonPill;
import com.typesafe.config.Config;
import de.hpi.akka_tutorial.remote.actors.*;
import de.hpi.akka_tutorial.util.AkkaUtils;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Calculator {

	private static final String DEFAULT_MASTER_SYSTEM_NAME = "MasterActorSystem";
	private static final String DEFAULT_SLAVE_SYSTEM_NAME = "SlaveActorSystem";

	public static void runMaster(String host, int port, int numLocalWorkers) {
		
		// Create the ActorSystem
		final Config config = AkkaUtils.createRemoteAkkaConfig(host, port);
		final ActorSystem actorSystem = ActorSystem.create(DEFAULT_MASTER_SYSTEM_NAME, config);

		// Create the Reaper.
		actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

		// Create the Listener
		final ActorRef listener = actorSystem.actorOf(Listener.props(), Listener.DEFAULT_NAME);

		// Create the Master
		final ActorRef master = actorSystem.actorOf(Master.props(listener, numLocalWorkers), Master.DEFAULT_NAME);

		// Create the Shepherd
		final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(master), Shepherd.DEFAULT_NAME);

		// Read ranges from the console and process them
		final Scanner scanner = new Scanner(System.in);
		while (true) {
			try {
				// Sleep to reduce mixing of log messages with the regular stdout messages.
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
				
				// Read input
				System.out.println("> Enter \"<min>,<max>\" to analyze for primes, "
						+ "\"all\" to log all calculated primes, "
						+ "\"max\" to log the largest calculated prime, "
						+ "\"exit\" for shutdown: ");
				String line = scanner.nextLine();

				// Check for "all" command
				if (line.equals("all")) {
					listener.tell(new Listener.LogPrimesMessage(), ActorRef.noSender());
					continue;
				}
				
				// Check for "max" command
				if (line.equals("max")) {
					listener.tell(new Listener.LogMaxMessage(), ActorRef.noSender());
					continue;
				}
				
				// Check for correct range message
				String[] lineSplit = line.split(",");
				if (lineSplit.length != 2)
					break;

				// Extract start- and endNumber
				long startNumber = Long.valueOf(lineSplit[0]);
				long endNumber = Long.valueOf(lineSplit[1]);

				// Start the calculation
				master.tell(new Master.RangeMessage(startNumber, endNumber), ActorRef.noSender());
				
			} catch (NumberFormatException e) {
				// Ignore and shutdown
				break;
			} catch (Exception e) {
				// Print and exit
				e.printStackTrace();
				System.exit(-1);
			}
		}
		scanner.close();
		System.out.println("Stopping...");

		// Do not accept any new subscriptions
		shepherd.tell(PoisonPill.getInstance(), ActorRef.noSender()); // stop via message (asynchronously; all current messages in the queue are processed first but no new)

		// Tell the master that we will not send any further requests
		master.tell(new Master.ShutdownMessage(), ActorRef.noSender());

		// Await termination: The termination should be issued by the reaper
		try {
			Await.ready(actorSystem.whenTerminated(), Duration.Inf());
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("ActorSystem finished!");
	}

	public static void runSlave(String host, int port, String masterHost, int masterPort) {

		// Create the local ActorSystem
		final Config config = AkkaUtils.createRemoteAkkaConfig(host, port);
		final ActorSystem actorSystem = ActorSystem.create(DEFAULT_SLAVE_SYSTEM_NAME, config);

		// Create the reaper.
		actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

		// Create a Slave
		final ActorRef slave = actorSystem.actorOf(Slave.props(), Slave.DEFAULT_NAME);

		// Tell the Slave to register the local ActorSystem
		slave.tell(new Slave.AddressMessage(new Address("akka.tcp", DEFAULT_MASTER_SYSTEM_NAME, masterHost, masterPort)), ActorRef.noSender());
	}
}
