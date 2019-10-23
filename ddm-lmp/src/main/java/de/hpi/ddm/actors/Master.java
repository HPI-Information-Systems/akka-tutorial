package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.Data;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props() {
		return Props.create(Master.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class EndMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final List<ActorRef> workers = new ArrayList<>();
	private final ActorRef largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	private final BloomFilter data = new BloomFilter(BloomFilter.DEFAULT_SIZE, true);
	
	private boolean isEnded = false;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(EndMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}
	
	protected void handle(EndMessage message) {
		this.isEnded = true;
		
		for (ActorRef worker : this.workers)
			worker.tell(PoisonPill.getInstance(), this.self());
		
		if (this.workers.isEmpty())
			this.self().tell(PoisonPill.getInstance(), this.self());
	}
	
	protected void handle(RegistrationMessage message) {
		if (this.isEnded) {
			this.sender().tell(PoisonPill.getInstance(), this.self());
			return;
		}
		
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());
		
		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(this.data, this.sender()), this.self());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		
		if (this.workers.isEmpty())
			this.self().tell(PoisonPill.getInstance(), this.self());
	}
}
