package de.hpi.akka_tutorial.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.akka_tutorial.messages.NumbersMessage;

public class PrimeListener extends AbstractActor {
	
	public static Props props() {
		return Props.create(PrimeListener.class);
	}
	
	private final LoggingAdapter logger = Logging.getLogger(this.getContext().getSystem(), this);
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(NumbersMessage.class, this::handle)
				.matchAny(object -> this.logger.info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(NumbersMessage message) {
		System.out.println(((NumbersMessage)message).toString());
		
		this.getContext().system().terminate();
	}
}
