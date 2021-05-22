package de.hpi.ddm.systems;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.ddm.actors.Reaper;
import de.hpi.ddm.actors.Worker;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.singletons.ConfigurationSingleton;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class WorkerSystem {

	public static final String WORKER_ROLE = "worker";
	
	public static void start() {
		final Configuration configSingleton = ConfigurationSingleton.get();
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.artery.canonical.hostname = \"" + configSingleton.getHost() + "\"\n" +
				"akka.remote.artery.canonical.port = " + configSingleton.getPort() + "\n" +
				"akka.cluster.roles = [" + WORKER_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + configSingleton.getActorSystemName() + "@" + configSingleton.getMasterHost() + ":" + configSingleton.getMasterPort() + "\"]")
			.withFallback(ConfigFactory.load("application"));
		
		final ActorSystem system = ActorSystem.create(configSingleton.getActorSystemName(), config);
		
	//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
	//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		
		ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < configSingleton.getNumWorkers(); i++){
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
				}
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
