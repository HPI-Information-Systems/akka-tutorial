package de.hpi.akka_tutorial.remote.actors;

import akka.actor.*;
import akka.remote.DisassociatedEvent;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Slave extends AbstractLoggingActor {

	public static String DEFAULT_NAME = "slave";

	/**
	 * Scheduling item to keep on trying to reconnect as regularly.
	 */
	private Cancellable connectSchedule;

	public static Props props() {
		return Props.create(Slave.class);
	}

	public static class Connect implements Serializable {

		private static final long serialVersionUID = -4399047760637406556L;

		private final Address address;

		public Connect(Address address) {
			this.address = address;
		}
	}


	/**
	 * This message signals that a connection request with a {@link Shepherd} actor was successful.
	 */
	public static class SubscriptionAcknowledgement implements Serializable {

	}

	public static class Shutdown implements Serializable {

		private static final long serialVersionUID = -8962039849767411379L;
	}


	@Override
	public void preStart() throws Exception {
		super.preStart();
		Reaper.watchWithDefaultReaper(this);

		// Listen for disassociation with the master.
//		this.getContext().getSystem().eventStream().subscribe(getSelf(), RemotingLifecycleEvent.class);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Connect.class, this::handle)
				.match(SubscriptionAcknowledgement.class, this::handle)
				.match(Shutdown.class, this::handle)
				.match(DisassociatedEvent.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\" ({})", object, object.getClass()))
				.build();
	}

	private void handle(Shutdown message) {

		// Log remote shutdown message
		this.log().info("Asked to stop.");

		// Shutdown this system
		self().tell(PoisonPill.getInstance(), self());
	}

	private void handle(Connect message) {
		if (this.connectSchedule != null) {
			this.connectSchedule.cancel();
			this.connectSchedule = null;
		}

		// Find the shepherd actor in the remote ActorSystem
		ActorSelection selection = this.getContext().system().actorSelection(String.format("%s/user/%s", message.address, Shepherd.DEFAULT_NAME));

		// Register the local ActorSystem by sending a subscription message
		Scheduler scheduler = getContext().getSystem().scheduler();
		ExecutionContextExecutor dispatcher = getContext().getSystem().dispatcher();
		this.connectSchedule = scheduler.schedule(
				Duration.Zero(),
				Duration.create(5, TimeUnit.SECONDS),
				() -> selection.tell(new Shepherd.SubscriptionMessage(message.address), this.getSelf()),
				dispatcher
		);
	}

	private void handle(SubscriptionAcknowledgement message) {
		if (this.connectSchedule != null) {
			this.connectSchedule.cancel();
			this.connectSchedule = null;
		}

		log().info("Successfully acknowledged by {}.", getSender());
	}

	private void handle(DisassociatedEvent event) {
		log().error("Disassociated from master. Stopping...");
		getContext().stop(getSelf());
	}

}