package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;

/**
 * The shepherd lives in the master actor system and waits for slave subscriptions.
 */
public class Shepherd extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "shepherd";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link Shepherd} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props(final ActorRef master) {
		return Props.create(Shepherd.class, () -> new Shepherd(master));
	}
	
	/**
	 * Asks the {@link Shepherd} to subscribe the sender and its (remote) actor system as a slave to this actor system.
	 */
	public static class SubscriptionMessage implements Serializable {
		
		private static final long serialVersionUID = 6122957437037004535L;
	}
	
	// A reference to the master actor that spawns new workers upon the connection of new actor systems
	private final ActorRef master;

	// A reference to all remote slave actors that subscribed to this shepherd
	private final Set<ActorRef> slaves = new HashSet<>();
	
	/**
	 * Construct a new {@link Shepherd} object.
	 * 
	 * @param master a reference to an {@link Master} actor to send addresses of subscribed actor systems to
	 */
	public Shepherd(final ActorRef master) {
		this.master = master;
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}
	
	@Override
	public void postStop() throws Exception {
		super.postStop();
		
		// Stop all slaves that connected to this Shepherd
		for (ActorRef slave : this.slaves)
			slave.tell(PoisonPill.getInstance(), this.getSelf());

		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(SubscriptionMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(SubscriptionMessage message) {
		
		// Find the sender of this message
		ActorRef slave = this.getSender();

		// Keep track of all subscribed slaves but avoid double subscription.
		if (!this.slaves.add(slave)) 
			return;
		this.log().info("New subscription: " + slave);

		// Acknowledge the subscription.
		slave.tell(new Slave.AcknowledgementMessage(), this.getSelf());

		// Set the subscriber on the watch list to get its Terminated messages
		this.getContext().watch(slave);

		// Extract the remote system's address from the sender.
		Address remoteAddress = this.getSender().path().address();

		// Inform the master about the new remote system.
		this.master.tell(new Master.RemoteSystemMessage(remoteAddress), this.getSelf());
	}
	
	private void handle(ShutdownMessage message) {
		
		// Shutdown all slaves that connected to this Shepherd
		for (ActorRef slave : this.slaves)
			slave.tell(new ShutdownMessage(), this.getSelf());
		
		// Stop accepting new slaves (to do so, the actor can simply stop itself)
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}
	
	private void handle(Terminated message) {
		
		// Find the sender of this message
		final ActorRef sender = this.getSender();
		
		// Remove the sender from the slaves list
		this.slaves.remove(sender);
	}
}
