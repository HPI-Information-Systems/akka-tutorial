package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;

public class Listener extends AbstractLoggingActor {
	
	public static Props props() {
		return Props.create(Listener.class);
	}
	
	public static class StringsMessage implements Serializable {
		
		private static final long serialVersionUID = -1779142448823490939L;
		
		private final List<String> strings;

		public List<String> getStrings() {
			return this.strings;
		}

		public StringsMessage(final List<String> strings) {
			this.strings = strings;
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StringsMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(StringsMessage stringsMessage) {
		
		this.log().info(this.asString(stringsMessage.getStrings()));
	}
	
	private String asString(List<String> strings) {
		
		StringBuilder builder = new StringBuilder("[");
		for (int i = 0; i < strings.size() - 1; i++)
			builder.append(strings.get(i) + ",");
		builder.append(strings.get(strings.size() - 1) + "]");
		return builder.toString();
	}
	
}
