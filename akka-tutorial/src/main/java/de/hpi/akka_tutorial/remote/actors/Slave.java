package de.hpi.akka_tutorial.remote.actors;

import akka.actor.*;
import akka.remote.DisassociatedEvent;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

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

		private final Address address;

		public AddressMessage(Address address) {
			this.address = address;
		}
	}

	/**
	 * Asks the {@link Slave} to acknowledge a successful connection request with a (remote) {@link Shepherd} actor.
	 */
	public static class AcknowledgementMessage implements Serializable {

		private static final long serialVersionUID = 2289467879887081348L;

	}

	/**
	 * Asks the {@link Slave} to stop itself.
	 */
	public static class ShutdownMessage implements Serializable {

		private static final long serialVersionUID = -8962039849767411379L;
	}

	/**
	 * Scheduling item to keep on trying to reconnect as regularly.
	 */
	private Cancellable connectSchedule;

	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);

		// Listen for disassociation with the master
		this.getContext().getSystem().eventStream().subscribe(getSelf(), DisassociatedEvent.class);
	}


	@Override
	public void postStop() throws Exception {
		super.postStop();
		log().info("Stopping {}...", self());
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
		this.log().info("Asked to stop.");

		// Shutdown this system
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}

	private void handle(AddressMessage message) {
		
		// Cancel any running connect schedule
		if (this.connectSchedule != null) {
			this.connectSchedule.cancel();
			this.connectSchedule = null;
		}

		// Find the shepherd actor in the remote ActorSystem
		final ActorSelection selection = this.getContext().system().actorSelection(String.format("%s/user/%s", message.address, Shepherd.DEFAULT_NAME));

		// Register the local ActorSystem by periodically sending subscription messages (until an acknowledgement was received)
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
		if (this.connectSchedule != null) {
			this.connectSchedule.cancel();
			this.connectSchedule = null;
		}

		this.log().info("Successfully acknowledged by {}.", getSender());
	}

	private void handle(DisassociatedEvent event) {
		if (this.connectSchedule == null) {
			// The disassociation is a problem, once we have already connected. Before that, it's no problem, though.
			this.log().error("Disassociated from master. Stopping...");
			this.getContext().stop(getSelf());
		}
	}

}