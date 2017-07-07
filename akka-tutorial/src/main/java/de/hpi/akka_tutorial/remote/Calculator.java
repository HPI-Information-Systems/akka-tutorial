package de.hpi.akka_tutorial.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import de.hpi.akka_tutorial.remote.actors.Listener;
import de.hpi.akka_tutorial.remote.actors.Master;
import de.hpi.akka_tutorial.remote.actors.Shepherd;
import de.hpi.akka_tutorial.remote.actors.Shepherd.Subscription;
import de.hpi.akka_tutorial.remote.actors.Slave;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class Calculator {

	private static final String masterSystemName = "MasterActorSystem";
	private static final String slaveSystemName = "SlaveActorSystem";
	private static final String shepherdName = "shepherd";
	private static final String masterName = "master";
	private static final String listenerName = "listener";
	private static final String slaveName = "slave";

	public static void runMaster(String host, int port) {
		// Create the ActorSystem
		final Config config = ConfigFactory.parseString("akka.actor.provider = remote")
			//	.withFallback(ConfigFactory.parseString("akka.actor.serializers.proto = \"akka.remote.serialization.ProtobufSerializer\""))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"java.io.Serializable\" = none"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Worker.NumbersMessage\" = proto"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Master.ObjectMessage\" = proto"))
				.withFallback(ConfigFactory.parseString("akka.remote.enabled-transports = [\"akka.remote.netty.tcp\"]"))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname = " + host))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.port = " + port))
				.withFallback(ConfigFactory.load("common"));
		final ActorSystem actorSystem = ActorSystem.create(masterSystemName, config);

		// Create the Listener
		final ActorRef listener = actorSystem.actorOf(Listener.props(), listenerName);

		// Create the Master
		final ActorRef master = actorSystem.actorOf(Master.props(listener), masterName);

		// Create the Shepherd
		final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(master), shepherdName);
		
		// Read ranges from the console and process them
		final Scanner scanner = new Scanner(System.in);
		while (true) {
			
			// Read input
			String line = scanner.nextLine();
			
			// Check for correct range message
			String[] lineSplit = line.split(",");
			if (lineSplit.length != 2)
				break;
			
			// Extract start- and endNumber
			long startNumber = Long.valueOf(lineSplit[0]);
			long endNumber = Long.valueOf(lineSplit[1]);
			
			// Start the calculation
			master.tell(new Master.RangeMessage(startNumber, endNumber), ActorRef.noSender());
		}
		scanner.close();
		
		// Shutdown the ActorSystem
		
		shepherd.tell(PoisonPill.getInstance(), ActorRef.noSender()); // stop via message (asynchronously; all current messages in the queue are processed first but no new)
	//	actorSystem.stop(shepherd); // stop via call (only the current message is processed but no further messages from the queue)

		// TODO
//		master.tell(Shutdown.class, sender); // Finish worker, stop worker, poisenpill listener
		
		// now the reaper will terminate the actorsystem
		
		actorSystem.terminate(); // TODO: Careful shutdown
		
		// Await termination
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
		final Config config = ConfigFactory.parseString("akka.actor.provider = remote")
			//	.withFallback(ConfigFactory.parseString("akka.actor.serializers.proto = \"akka.remote.serialization.ProtobufSerializer\""))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"java.io.Serializable\" = none"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Worker.NumbersMessage\" = proto"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Master.ObjectMessage\" = proto"))
				.withFallback(ConfigFactory.parseString("akka.remote.enabled-transports = [\"akka.remote.netty.tcp\"]"))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname = " + host))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.port = " + port))
				.withFallback(ConfigFactory.load("common"));
		final ActorSystem actorSystem = ActorSystem.create(slaveSystemName, config);

		// Create a Slave
		final ActorRef slave = actorSystem.actorOf(Slave.props(), slaveName);
		
		// Tell the Slave to register the local ActorSystem
		slave.tell(new Slave.Connect(masterSystemName, masterHost, masterPort, shepherdName, slaveSystemName, host, port), ActorRef.noSender());
	}
}
