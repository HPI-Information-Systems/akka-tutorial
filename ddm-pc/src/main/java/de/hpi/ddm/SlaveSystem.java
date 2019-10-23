package de.hpi.ddm;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.ddm.actors.Reaper;
import de.hpi.ddm.actors.Worker;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class SlaveSystem {

	public static final String SLAVE_ROLE = "slave";
	
	public static void start() {
		final Configuration c = ConfigurationSingleton.get();
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.artery.canonical.hostname = \"" + c.getHost() + "\"\n" +
				"akka.remote.artery.canonical.port = " + c.getPort() + "\n" +
				"akka.cluster.roles = [" + SLAVE_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + c.getActorSystemName() + "@" + c.getMasterHost() + ":" + c.getMasterPort() + "\"]")
			.withFallback(ConfigFactory.load("application"));
		
		final ActorSystem system = ActorSystem.create(c.getActorSystemName(), config);
		
	//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
	//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		
		ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
		
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
	}
}
