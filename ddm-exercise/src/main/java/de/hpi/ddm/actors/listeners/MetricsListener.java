package de.hpi.ddm.actors.listeners;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.metrics.ClusterMetricsChanged;
import akka.cluster.metrics.NodeMetrics;
import akka.cluster.metrics.StandardMetrics;
import akka.cluster.metrics.StandardMetrics.HeapMemory;
import akka.cluster.metrics.StandardMetrics.Cpu;
import akka.cluster.metrics.ClusterMetricsExtension;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class MetricsListener extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "metricsListener";

	public static Props props() {
		return Props.create(MetricsListener.class);
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private final Cluster cluster = Cluster.get(getContext().system());
	private final ClusterMetricsExtension extension = ClusterMetricsExtension.get(getContext().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.extension.subscribe(self());
	}

	@Override
	public void postStop() {
		this.extension.unsubscribe(self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
			.match(ClusterMetricsChanged.class, this::logMetrics)
			.match(CurrentClusterState.class, message -> {/*Ignore*/})
			.build();
	}
	
	private void logMetrics(ClusterMetricsChanged clusterMetrics) {
		for (NodeMetrics nodeMetrics : clusterMetrics.getNodeMetrics()) {
			if (nodeMetrics.address().equals(this.cluster.selfAddress())) {
				this.logHeap(nodeMetrics);
				this.logCpu(nodeMetrics);
			}
		}
	}

	private void logHeap(NodeMetrics nodeMetrics) {
		HeapMemory heap = StandardMetrics.extractHeapMemory(nodeMetrics);
		if (heap != null) {
			this.log.info("Used heap: {} MB", ((double) heap.used()) / 1024 / 1024);
		}
	}

	private void logCpu(NodeMetrics nodeMetrics) {
		Cpu cpu = StandardMetrics.extractCpu(nodeMetrics);
		if (cpu != null && cpu.systemLoadAverage().isDefined()) {
			this.log.info("Load: {} ({} processors)", cpu.systemLoadAverage().get(), cpu.processors());
		}
	}
}
