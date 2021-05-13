package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Arrays;

import com.twitter.chill.KryoPool;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
	private ActorRef sender = null;
	
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
				.match(ActorRef[].class, this::handleCommPartners)
				.match(byte[].class, this::handlePart)
				.match(Boolean.class, this::handleDone)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}
	private byte[][] divideArray(byte[] source, int chunksize) {

        byte[][] ret = new byte[(int)Math.ceil(source.length / (double)chunksize)][chunksize];


        for(int i = 0; i < ret.length; i++) {
			int end = i == ret.length ? source.length : (i+1) * chunksize;
            ret[i] = Arrays.copyOfRange(source, i * chunksize, end);
        }

        return ret;
    }

	private void handleLargeMessage(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		receiverProxy.tell(new ActorRef[]{sender, receiver}, this.self());

		KryoPool kryo = KryoPoolSingleton.get();
		byte[] all = kryo.toBytesWithClass(message);
		byte[][] parts = this.divideArray(all, 1024);

		for (byte[] part : parts) {
			receiverProxy.tell(part, this.self());
		}

		receiverProxy.tell(true, this.self());
	}

	private void handlePart(byte[] part) {
		byte[] old = this.received;
		this.received = new byte[old.length + part.length];
		System.arraycopy(old, 0, this.received, 0, old.length);
		System.arraycopy(part, 0, this.received, old.length, part.length);
	}

	private void handleCommPartners(ActorRef[] commPartners) {
		this.sender = commPartners[0];
		this.receiver = commPartners[1];
	}

	private void handleDone(boolean tirggerForward) {
		KryoPool kryo = KryoPoolSingleton.get();

		this.receiver.tell(kryo.fromBytes(this.received), this.sender);
	}
}
