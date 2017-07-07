package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;

public class Shepherd extends AbstractLoggingActor {

	public static Props props(final ActorRef master) {
		return Props.create(Shepherd.class, () -> new Shepherd(master));
	}
	
	public static class Subscription implements Serializable {
		
		private static final long serialVersionUID = 6122957437037004535L;

		private final String name;
		private final String ip;
		private final int port;

		public String getName() {
			return this.name;
		}

		public String getIp() {
			return this.ip;
		}

		public int getPort() {
			return this.port;
		}

		public Subscription(final String name, final String ip, final int port) {
			this.name = name;
			this.ip = ip;
			this.port = port;
		}
		
		@Override
		public String toString() {
			return "akka.tcp://" + this.getName() + "@" + this.getIp() + ":" + this.getPort();
		}
	}
	
	private final ActorRef master;
	private final List<ActorRef> slaves = new ArrayList<>();
	
	public Shepherd(final ActorRef master) {
		this.master = master;
	}
	
	@Override
	public void postStop() throws Exception {
		
		// Shutdown all slaves
		for (ActorRef slave : this.slaves)
			slave.tell(new Slave.Shutdown(), this.getSelf());
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Subscription.class, this::handle)
				.match(Terminated.class, s -> this.slaves.remove(this.getSender()))
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(Subscription message) {
		
		// Set the subscriber on the watch list to get its Terminated messages
		this.getContext().watch(this.getSender());
		
		// Set the subscriber on the slaves list to terminate its ActorSystem if this ActorSystem terminates
		this.slaves.add(this.getSender());
		
		// Tell the master about the new slave
		this.master.tell(new Master.URIMessage(message.toString()), this.getSelf()); 
		
		// Log the subscription
		this.log().info("New subscription: " + message.toString());
	}
}
