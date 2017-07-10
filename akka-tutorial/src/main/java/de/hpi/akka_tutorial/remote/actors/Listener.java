package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;

public class Listener extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "listener";

	public static Props props() {
		return Props.create(Listener.class);
	}

	/**
	 * Asks the listener to print the given message.
	 */
	public static class PrintMessage implements Serializable {
		
		private static final long serialVersionUID = -1779142448823490939L;
		
		private final String message;

		public PrintMessage(final String message) {
			this.message = message;
		}
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
		log().info("Stopping {}...", self());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PrintMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(PrintMessage printMessage) {
		System.out.println(printMessage.message);
	}

}
