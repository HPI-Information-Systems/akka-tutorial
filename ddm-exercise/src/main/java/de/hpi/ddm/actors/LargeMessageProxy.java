package de.hpi.ddm.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.twitter.chill.KryoPool;
import de.hpi.ddm.singletons.KryoPoolSingleton;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	private byte[] received = new byte[0];
	private ActorRef receiver = null;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handleLargeMessage)
				.match(ActorRef.class, this::handleReceiver)
				.match(byte[].class, this::handlePart)
				.match(Boolean.class, this::handleDone)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}
	private byte[][] divideArray(byte[] source, int chunksize) {

        byte[][] ret = new byte[(int)Math.ceil(source.length / (double)chunksize)][chunksize];

        int start = 0;

        for(int i = 0; i < ret.length; i++) {
            ret[i] = Arrays.copyOfRange(source,start, start + chunksize);
            start += chunksize ;
        }

        return ret;
    }

	private void handleLargeMessage(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		receiverProxy.tell(receiver, this.self());

		KryoPool kryo = KryoPoolSingleton.get();
		byte[] all = kryo.serialize(message);
		byte[][] parts = this.divideArray(all, 1024);

		for (byte[] part : parts) {
			receiverProxy.tell(part, this.self());
		}

		receiverProxy.tell(true, this.self());
	}

	private void handlePart(byte[] part) {
		byte[] old = this.received;
		this.received = new byte[old.length + part.length];
		System.arraycopy(old, 0, this.recieved, 0);
		System.arraycopy(part, 0, this.recieved, old.length);
	}

	private void handleReceiver(ActorRef receiver) {
		this.receiver = receiver;
	}

	private void handleDone(boolean tirggerForward) {
		KryoPool kryo = KryoPoolSingleton.get();

		message.getReceiver().tell(kryo.deserialize(this.received), this.receiver);
		this.received = new byte[0];
		this.receiver = null;
	}
}
