package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Arrays;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.event.Logging;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.TreeMap;
import java.nio.ByteBuffer;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	public static final int MESSAGE_SIZE = 2;

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
		private int messageNumber;
		private int byteHash;
		private int messageLength;
		private int numberOfMessages;
	}

	private HashMap<Integer, TreeMap<Integer, byte[]>> messagesToReceiveMap;

	/////////////////
	// Actor State //
	/////////////////

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		byte[] byteMessage = KryoPoolSingleton.get().toBytesWithClass(message);
		int byteHash = Arrays.hashCode(byteMessage);
		int messageLength = byteMessage.length;
		int numberOfMessages = (messageLength + 1) / MESSAGE_SIZE;

		for (int messageNumber = 0; messageNumber * MESSAGE_SIZE < messageLength; messageNumber++) {

			int startByte = messageNumber * MESSAGE_SIZE;
			int endByte = (messageNumber + 1) * MESSAGE_SIZE;
			if (messageLength < endByte) endByte = messageLength;

			byte[] bytesToSend = Arrays.copyOfRange(byteMessage, startByte, endByte);
			receiverProxy.tell(new BytesMessage<>(bytesToSend, sender, receiver, messageNumber, byteHash, messageLength, numberOfMessages), this.self());
		}


	}

	private void handle(BytesMessage<?> message) {
		int messageNumber = message.getMessageNumber();
		int numberOfMessages = message.getNumberOfMessages();
		int byteHash = message.getByteHash();

		if (messagesToReceiveMap == null) messagesToReceiveMap = new HashMap<>();

		if (!messagesToReceiveMap.containsKey(byteHash)) {
			messagesToReceiveMap.put(byteHash, new TreeMap<>());
		}

		// add new chunk to chunkMap
		TreeMap<Integer, byte[]> chunkMap = messagesToReceiveMap.get(byteHash);

		if (!chunkMap.containsKey(messageNumber)) {
			chunkMap.put(messageNumber, (byte[]) message.getBytes());
		}

		if (chunkMap.size() == numberOfMessages) {
			byte[] rebuildMessage = new byte[message.getMessageLength()];
			ByteBuffer bytesToAppend = ByteBuffer.wrap(rebuildMessage);
			for (byte[] bytes : chunkMap.values()) {
				bytesToAppend.put(bytes);
			}

			if (Arrays.hashCode(rebuildMessage) == byteHash) {

				Object messageDeserialized = KryoPoolSingleton.get().fromBytes(rebuildMessage);

				message.getReceiver().tell(messageDeserialized, message.getSender());
			}
		}
	}
}
