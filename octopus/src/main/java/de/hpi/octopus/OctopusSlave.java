package de.hpi.octopus;

import com.typesafe.config.Config;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.MetricsListener;

public class OctopusSlave extends OctopusSystem {

	public static final String SLAVE_ROLE = "slave";
	
	public static void start(String actorSystemName, int workers, String host, int port, String masterhost, int masterport) {
		
		final Config config = createConfiguration(actorSystemName, SLAVE_ROLE, host, port, masterhost, masterport);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				//system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
			}
		});
	}
}
