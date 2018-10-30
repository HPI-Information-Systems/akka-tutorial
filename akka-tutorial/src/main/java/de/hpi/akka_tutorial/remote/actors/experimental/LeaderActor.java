package de.hpi.akka_tutorial.remote.actors.experimental;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class LeaderActor extends AbstractLoggingActor {

	public static Props props(int identifier) {
		return Props.create(LeaderActor.class, () -> new LeaderActor(identifier));
	}
	
	public static class LamportTimestampedValue implements Serializable {
		private static final long serialVersionUID = 1L;
		public String value;
		public int counter;
		public int identifier;
		public LamportTimestampedValue(String value, int counter, int identifier) {
			this.counter = counter;
			this.identifier = identifier;
			this.value = value;
		}
	}
	
	public static class ReadMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public int key;
		public ReadMessage(int key) {
			this.key = key;
		}
	}
	
	public static class ReadResponseMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public String value;
		public ReadResponseMessage(String value) {
			this.value = value;
		}
	}

	public static class WriteMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public int key;
		public String value;
		public int counter;
		public WriteMessage(int key, String value, int counter) {
			this.key = key;
			this.value = value;
			this.counter = counter;
		}
	}
	
	public static class WriteResponseMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public int counter;
		public WriteResponseMessage(int counter) {
			this.counter = counter;
		}
	}
	
	public static class PropagationMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		public int key;
		public LamportTimestampedValue value;
		public PropagationMessage(int key, LamportTimestampedValue value) {
			this.key = key;
			this.value = value;
		}
	}
	
	private final Map<Integer, LamportTimestampedValue> replica = new HashMap<>();
	private final List<ActorRef> otherLeaders = new ArrayList<>();
	private final int identifier;
	
	public LeaderActor(final int identifier) {
		this.identifier = identifier;
	}
	
	public void addLeaderRef(ActorRef otherLeader) {
		this.otherLeaders.add(otherLeader);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ReadMessage.class, this::handle)
				.match(WriteMessage.class, this::handle)
				.match(PropagationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(ReadMessage message) {
		this.getSender().tell(new ReadResponseMessage(this.replica.get(message.key).value), this.getSelf());
	}
	
	private void handle(WriteMessage message) {
		LamportTimestampedValue oldValue = this.replica.get(message.key);
		
		int counter = (oldValue == null) ? message.counter : Math.max(oldValue.counter, message.counter);
		counter++;
		
		if (oldValue == null || 
				oldValue.counter < message.counter || 
				(oldValue.counter == message.counter && oldValue.identifier < this.identifier)) {
			LamportTimestampedValue newValue = new LamportTimestampedValue(message.value, counter, this.identifier);
			
			this.replica.put(message.key, newValue);
		
			for (ActorRef otherLeader : this.otherLeaders)
				otherLeader.tell(new PropagationMessage(message.key, newValue), this.getSelf());
			this.getSender().tell(new WriteResponseMessage(counter), this.getSelf());
		}
	}
	
	private void handle(PropagationMessage message) {
		LamportTimestampedValue oldValue = this.replica.get(message.key);
		
		if (oldValue == null || 
				oldValue.counter < message.value.counter || 
				(oldValue.counter == message.value.counter && oldValue.identifier < message.value.identifier)) {
			this.replica.put(message.key, message.value);
		}
	}
}
