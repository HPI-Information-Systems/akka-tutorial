package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import akka.remote.artery.SystemMessageDelivery;
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

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class PrepareLargeMessage implements Serializable {
		private static final long serialVersionUID = 18898612711109598L;
		private ActorRef receiver;
		private ActorRef sender;
		private int messageId;
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class AcknowledgePrepareLargeMessage implements Serializable {
		private static final long serialVersionUID = 18898612711109598L;
		private int messageId;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private byte[] bytes;
		private int partId;
		private int messageId;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class AcknowledgeBytesMessage implements Serializable {
		private static final long serialVersionUID = 4052807743789419842L;
		private int partId;
		private int messageId;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessageFinishedMessage implements Serializable {
		private static final long serialVersionUID = 4052807743789418888L;
		private int messageId;
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class LargeMessageBuffer {
		private ActorRef receiver;
		private ActorRef sender;
		private byte[] messageBuffer;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	private Map<Integer, byte[][]> outgoingMessagesMap = new HashMap();
	private Map<Integer, LargeMessageBuffer> incomingMessagesMap = new HashMap();
	private int maxOutgoingMessageId = 0;
	
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
				.match(PrepareLargeMessage.class, this::handlePrepareLargeMessage)
				.match(AcknowledgePrepareLargeMessage.class, this::handleAcknowledgePrepareLargeMessage)
				.match(BytesMessage.class, this::handleBytesMessage)
				.match(AcknowledgeBytesMessage.class, this::handleAcknowledgeBytesMessage)
				.match(LargeMessageFinishedMessage.class, this::handleLargeMessageFinishedMessage)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private byte[][] divideArray(byte[] source, int chunksize) {

        byte[][] ret = new byte[(int)Math.ceil(source.length / (double)chunksize)][chunksize];

	    int part = 0;
        for(; part < ret.length - 1; part++) {
			int end = (part + 1) * chunksize;
            ret[part] = Arrays.copyOfRange(source, part * chunksize, end);
        }
        // The last part should only contain as many bytes as needed.
        int end = source.length;
        ret[part] = Arrays.copyOfRange(source, part * chunksize, end);

        return ret;
    }

	private void handleLargeMessage(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));


		KryoPool kryo = KryoPoolSingleton.get();
		byte[] all = kryo.toBytesWithClass(message);
		// Default max byte size per message is 262144 according to mail. Could not find anything about this on the internet.
		byte[][] parts = this.divideArray(all, 262144);
		outgoingMessagesMap.put(maxOutgoingMessageId, parts);
		receiverProxy.tell(new PrepareLargeMessage(receiver, sender, maxOutgoingMessageId), this.self());
		maxOutgoingMessageId++;
	}

	private void handlePrepareLargeMessage(PrepareLargeMessage message){
		this.incomingMessagesMap.put(message.messageId, new LargeMessageBuffer(message.receiver, message.sender, new byte[0]));
		this.getSender().tell(new AcknowledgePrepareLargeMessage(message.messageId), this.self());
	}

	private void handleAcknowledgePrepareLargeMessage(AcknowledgePrepareLargeMessage message){
		int messageId = message.messageId;
		// send first bytes
		this.sendNextFewBytes(messageId, 0);
	}

	private void handleAcknowledgeBytesMessage(AcknowledgeBytesMessage message){
		int messageId = message.messageId;
		int partId = message.partId;
		// send next few bytes
		this.sendNextFewBytes(messageId, partId + 1);
	}

	private void handleBytesMessage(BytesMessage message){
		LargeMessageBuffer buffer = incomingMessagesMap.get(message.messageId);
		byte[] part = message.bytes;
		byte[] old = buffer.messageBuffer;
		buffer.messageBuffer = new byte[old.length + part.length];
		System.arraycopy(old, 0, buffer.messageBuffer, 0, old.length);
		System.arraycopy(part, 0, buffer.messageBuffer, old.length, part.length);
		this.getSender().tell(new AcknowledgeBytesMessage(message.partId, message.messageId), this.self());
	}

	private void sendNextFewBytes(int messageId, int partIndex){
		byte[][] bytes = this.outgoingMessagesMap.get(messageId);
		// send first bytes
		if(partIndex < bytes.length) {
			this.getSender().tell(new BytesMessage(bytes[partIndex], partIndex, messageId), this.self());
		} else {
			// Send all message parts!
			this.getSender().tell(new LargeMessageFinishedMessage(messageId), this.self());
			this.outgoingMessagesMap.remove(messageId);
		}
	}

	private void handleLargeMessageFinishedMessage(LargeMessageFinishedMessage message){
		LargeMessageBuffer buffer = incomingMessagesMap.get(message.messageId);
		incomingMessagesMap.remove(buffer);
		KryoPool kryo = KryoPoolSingleton.get();
		buffer.receiver.tell(kryo.fromBytes(buffer.messageBuffer), buffer.sender);
	}
}
