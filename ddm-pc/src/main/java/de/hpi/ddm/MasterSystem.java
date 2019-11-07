package de.hpi.ddm;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.ddm.actors.Collector;
import de.hpi.ddm.actors.Master;
import de.hpi.ddm.actors.Reader;
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

	//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
	//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		
		ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
		
		ActorRef reader = system.actorOf(Reader.props(), Reader.DEFAULT_NAME);
		
		ActorRef collector = system.actorOf(Collector.props(), Collector.DEFAULT_NAME);
		
		ActorRef master = system.actorOf(Master.props(reader, collector), Master.DEFAULT_NAME);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < c.getNumWorkers(); i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
				
				if (!c.isStartPaused())
					system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.StartMessage(), ActorRef.noSender());
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
		
		if (c.isStartPaused()) {
			System.out.println("Press <enter> to start!");
			try (final Scanner scanner = new Scanner(System.in)) {
				scanner.nextLine();
			}
			
			system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.StartMessage(), ActorRef.noSender());
		}
	}
}
