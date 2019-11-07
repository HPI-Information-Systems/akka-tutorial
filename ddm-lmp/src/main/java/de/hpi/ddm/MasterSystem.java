package de.hpi.ddm;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.ddm.actors.Master;
import de.hpi.ddm.actors.Reaper;
import de.hpi.ddm.actors.Worker;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class MasterSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start() {
		final Configuration c = ConfigurationSingleton.get();
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.artery.canonical.hostname = \"" + c.getHost() + "\"\n" +
				"akka.remote.artery.canonical.port = " + c.getPort() + "\n" +
				"akka.cluster.roles = [" + MASTER_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + c.getActorSystemName() + "@" + c.getHost() + ":" + c.getPort() + "\"]")
			.withFallback(ConfigFactory.load("application"));
		
		final ActorSystem system = ActorSystem.create(c.getActorSystemName(), config);
		
		ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
		
		ActorRef master = system.actorOf(Master.props(), Master.DEFAULT_NAME);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < c.getNumWorkers(); i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
			}
		});
		
		Cluster.get(system).registerOnMemberRemoved(new Runnable() {
			@Override
			public void run() {
				system.terminate();

				new Thread() {
					@Override
					public void run() {
						try {
							Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
						} catch (Exception e) {
							System.exit(-1);
						}
					}
				}.start();
			}
		});
		
		System.out.println("Press <enter> to end the application!");
		try (final Scanner scanner = new Scanner(System.in)) {
			scanner.nextLine();
		}
		
		system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.EndMessage(), ActorRef.noSender());
	}
}
