package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import akka.actor.*;

/**
 * The shepherd lives in the master actor system and waits for slave subscriptions.
 */
public class Shepherd extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "shepherd";

	public static Props props(final ActorRef master) {
		return Props.create(Shepherd.class, () -> new Shepherd(master));
	}
	
	public static class SubscriptionMessage implements Serializable {
		
		private static final long serialVersionUID = 6122957437037004535L;

		public SubscriptionMessage() {
		}

		@Override
		public String toString() {
			return "SubscriptionMessage";
		}
	}
	
	private final ActorRef master;

	private final Set<ActorRef> slaves = new HashSet<>();
	
	public Shepherd(final ActorRef master) {
		this.master = master;
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		Reaper.watchWithDefaultReaper(this);
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();
		// Shutdown all slaves
		for (ActorRef slave : this.slaves)
			slave.tell(new Slave.Shutdown(), this.getSelf());

		log().info("Stopping {}...", self());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(SubscriptionMessage.class, this::handle)
				.match(Terminated.class, s -> this.slaves.remove(this.getSender()))
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(SubscriptionMessage message) {
		ActorRef slave = this.getSender();

		// Keep track of all subscribed slaves but avoid double subscription.
		if (!this.slaves.add(slave)) return;
		this.log().info("New subscription: " + message.toString());

		// Acknowledge the subscription.
		slave.tell(new Slave.SubscriptionAcknowledgement(), this.getSelf());

		// Set the subscriber on the watch list to get its Terminated messages
		this.getContext().watch(slave);

		// Extract the remote system's address from the sender.
		Address remoteAddress = this.getSender().path().address();

		// Inform the master about the new remote system.
		this.master.tell(new Master.RemoteSystemMessage(remoteAddress), this.getSelf());

	}
}
