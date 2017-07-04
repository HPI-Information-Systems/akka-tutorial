package de.hpi.akka_tutorial.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import de.hpi.akka_tutorial.remote.actors.Listener;
import de.hpi.akka_tutorial.remote.actors.Master;
import de.hpi.akka_tutorial.remote.actors.Shepherd;
import de.hpi.akka_tutorial.remote.actors.Shepherd.Subscription;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class Calculator {

	private final String masterSystemName = "MasterActorSystem";
	private final String masterIP = "172.17.24.120";
	private final String masterPort = "2552";
	
	private final String slaveSystemName = "SlaveActorSystem";
	private final String slaveIP = "172.17.24.120";
	private final String slavePort = "2553";
	
	private final String shepherdName = "shepherd";
	private final String masterName = "master";
	private final String listenerName = "listener";
	
	private final List<Subscription> subscriptions = new ArrayList<>();
	
	public synchronized void addSubscription(Subscription subscription) {
		this.subscriptions.add(subscription);
	}
	
	public synchronized List<Subscription> getSubscriptions() {
		return new ArrayList<>(this.subscriptions);
	}
	
	public void calculate(final long startNumber, final long endNumber) {

	//	this.startMaster(startNumber, endNumber);

		this.startSlave();
	}

	private void startMaster(final long startNumber, final long endNumber) {

		// Create the ActorSystem
		final Config config = ConfigFactory.parseString("akka.actor.provider = remote")
			//	.withFallback(ConfigFactory.parseString("akka.actor.serializers.proto = \"akka.remote.serialization.ProtobufSerializer\""))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"java.io.Serializable\" = none"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Worker.NumbersMessage\" = proto"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Master.ObjectMessage\" = proto"))
				.withFallback(ConfigFactory.parseString("akka.remote.enabled-transports = [\"akka.remote.netty.tcp\"]"))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname = " + this.masterIP))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.port = " + this.masterPort))
				.withFallback(ConfigFactory.load("common"));
		final ActorSystem actorSystem = ActorSystem.create(this.masterSystemName, config);

		// Create the Shepherd
		final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(this), this.shepherdName);
		
		// Wait for subscriptions
		final Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		scanner.close();
		
		// Stop the Shepherd
		actorSystem.stop(shepherd);

		// Get current subscriptions
		final List<Subscription> subscriptions = this.getSubscriptions();
		
		// Create the Listener
		final ActorRef listener = actorSystem.actorOf(Listener.props(), this.listenerName);

		// Create the Master
		final ActorRef master = actorSystem.actorOf(Master.props(subscriptions, listener), this.masterName);

		// Start the calculation
		master.tell(new Master.RangeMessage(startNumber, endNumber), ActorRef.noSender());

		// Shutdown the ActorSystem
		try {
			Await.ready(actorSystem.whenTerminated(), Duration.Inf());
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("ActorSystem finished!");
	}

	private void startSlave() {

		// Create the local ActorSystem
		final Config config = ConfigFactory.parseString("akka.actor.provider = remote")
			//	.withFallback(ConfigFactory.parseString("akka.actor.serializers.proto = \"akka.remote.serialization.ProtobufSerializer\""))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"java.io.Serializable\" = none"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Worker.NumbersMessage\" = proto"))
			//	.withFallback(ConfigFactory.parseString("akka.actor.serialization-bindings.\"de.hpi.akka_tutorial.remote.actors.Master.ObjectMessage\" = proto"))
				.withFallback(ConfigFactory.parseString("akka.remote.enabled-transports = [\"akka.remote.netty.tcp\"]"))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname = " + this.slaveIP))
				.withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.port = " + this.slavePort))
				.withFallback(ConfigFactory.load("common"));
		final ActorSystem actorSystem = ActorSystem.create(this.slaveSystemName, config);

		// Find the shepherd actor in the remote ActorSystem
		ActorSelection selection = actorSystem.actorSelection("akka.tcp://" + this.masterSystemName + "@" + this.masterIP + ":" + this.masterPort + "/user/" + this.shepherdName);

		// Register the local ActorSystem by sending a subscription message
		selection.tell(new Shepherd.Subscription(this.slaveSystemName, this.slaveIP, this.slavePort), ActorRef.noSender());
	}
}
