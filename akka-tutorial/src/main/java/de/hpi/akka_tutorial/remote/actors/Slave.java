package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Scheduler;
import akka.remote.DisassociatedEvent;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.Duration;

/**
 * The slave actor tries to subscribe its actor system to a shepherd actor in a master actor system.
 */
public class Slave extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "slave";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link Slave} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(Slave.class);
	}

	/**
	 * Asks the {@link Slave} to subscribe to a (remote) {@link Shepherd} actor with a given address.
	 */
	public static class AddressMessage implements Serializable {

		private static final long serialVersionUID = -4399047760637406556L;

		private Address address;

		public AddressMessage(final Address address) {
			this.address = address;
		}
		
		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private AddressMessage() {
		}
	}

	/**
	 * Asks the {@link Slave} to acknowledge a successful connection request with a (remote) {@link Shepherd} actor.
	 */
	public static class AcknowledgementMessage implements Serializable {

		private static final long serialVersionUID = 2289467879887081348L;

	}

	// A scheduling item to keep on trying to reconnect as regularly
	private Cancellable connectSchedule;
	
	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);

		// Listen for disassociation with the master
		this.getContext().getSystem().eventStream().subscribe(this.getSelf(), DisassociatedEvent.class);
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();
		
		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(AddressMessage.class, this::handle)
				.match(AcknowledgementMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.match(DisassociatedEvent.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\" ({})", object, object.getClass()))
				.build();
	}

	private void handle(ShutdownMessage message) {

		// Log remote shutdown message
		this.log().info("Was asked to stop.");

		// Stop self by sending a poison pill
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}

	private void handle(AddressMessage message) {
		
		// Cancel any running connect schedule, because got a new address
		if (this.connectSchedule != null) {
			this.connectSchedule.cancel();
			this.connectSchedule = null;
		}

		// Find the shepherd actor in the remote actor system
		final ActorSelection selection = this.getContext().getSystem().actorSelection(String.format("%s/user/%s", message.address, Shepherd.DEFAULT_NAME));

		// Register the local actor system by periodically sending subscription messages (until an acknowledgement was received)
		final Scheduler scheduler = this.getContext().getSystem().scheduler();
		final ExecutionContextExecutor dispatcher = this.getContext().getSystem().dispatcher();
		this.connectSchedule = scheduler.schedule(
				Duration.Zero(),
				Duration.create(5, TimeUnit.SECONDS),
				() -> selection.tell(new Shepherd.SubscriptionMessage(), this.getSelf()),
				dispatcher
		);
	}

	private void handle(AcknowledgementMessage message) {
		
		// Cancel any running connect schedule, because we are now connected
		if (this.connectSchedule != null) {
			this.connectSchedule.cancel();
			this.connectSchedule = null;
		}

		// Log the connection success
		this.log().info("Subscription successfully acknowledged by {}.", this.getSender());
	}

	private void handle(DisassociatedEvent event) {
		
		// Disassociations are a problem only once we have a running connection, i.e., no connection schedule is active; they do not concern this actor otherwise.
		if (this.connectSchedule == null) {
			this.log().error("Disassociated from master. Stopping...");
			this.getContext().stop(this.getSelf());
		}
	}

}