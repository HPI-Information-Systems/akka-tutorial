package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	// Assumption: This is sent over TCP and Ethernet and the MTU is 1500.
	public static final int CHUNK_SIZE = 1024;

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	// Messages to be sent to the sender's proxy
	// =========================================

	@Data @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private int id;
		@NonNull private T message;
		@NonNull ActorRef receiver;

		LargeMessage(T message, ActorRef receiver) {
			this.message = message;
			this.receiver = receiver;
			this.id = ThreadLocalRandom.current().nextInt();
		}
	}

	/**
	 * A message to be sent to the sender's proxy to ask it to send the next chunk to the receiver's proxy.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	private static class RequestNextChunkMessage implements Serializable {
		private static final long serialVersionUID = -5374303325056263165L;
		private int id;
	}

	// Messages to be sent to the receiver's proxy
	// ===========================================

	/**
	 * A message to be sent to the receiver's proxy to let it know it can start asking the sender's proxy
	 * for the first chunk.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	private static class StartMessage implements Serializable {
		private static final long serialVersionUID = 2746268654101845915L;
		private int id;
		private int numBytesSerialized;
	}

	/**
	 * A message containing the next chunk.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	private static class NextChunkMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private int id;
		private byte[] payload;
	}

	/**
	 * A message to be sent to the receiver's proxy to let it know that no more chunks are left. The fields of this
	 * message contain the actual sender and receiver (i.e., not the proxies) which are used for forwarding the
	 * reassembled and deserialized message to the actual receiver. Also, this message contains the number of bytes
	 * of the serialized message so that the receiver's proxy can verify it has received all the bytes.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	private static class NoChunksLeftMessage implements Serializable {
		private static final long serialVersionUID = -4373062859625501319L;
		private int id;
		private ActorRef sender;
		private ActorRef receiver;
	}

	/////////////////
	// Actor State //
	/////////////////

	// Fields of the sender's proxy
	@Data @NoArgsConstructor
	class SendMessageState {
		private byte[] serMsg;
		private ActorRef sender, receiver;
		private ActorSelection receiverProxy;
		private int currChunkStartIx;
		@Getter
		@Accessors(fluent = true)
		private boolean hasSentAllChunks;
	}

	// Fields of the receiver's proxy
	@Data @NoArgsConstructor
	class ReceiveMessageState {
		private boolean started = false;
		private ByteArrayOutputStream buffer; // used for storing the bytes from the received chunks
		private int expectedBytes;
		@Accessors(fluent = true)
		private boolean hasSentDeserMsg;
	}

	HashMap<Integer, SendMessageState> sendMessageStates = new HashMap<Integer, SendMessageState>();
	HashMap<Integer, ReceiveMessageState> receiveMessageStates = new HashMap<Integer, ReceiveMessageState>();


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
				.match(LargeMessage.class, this::handle)
				.match(StartMessage.class, this::handle)
				.match(RequestNextChunkMessage.class, this::handle)
				.match(NextChunkMessage.class, this::handle)
				.match(NoChunksLeftMessage.class, this::handle)
				.matchAny(object -> this.log().debug("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) {
		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.
		this.log().debug("Received large message to forward.");
		Object message = largeMessage.getMessage();

		SendMessageState ms = new SendMessageState();
		// Save the references to the actual sender and receiver, as well as to the receiver's proxy into the fields
		// of the sender's proxy.
		ms.setSender(this.sender());
		ms.setReceiver(largeMessage.getReceiver());
		ms.setReceiverProxy(this.context().actorSelection(ms.receiver.path().child(DEFAULT_NAME)));

		// Serialize the message and initialize/reset the sender's proxy fields.
		ms.setSerMsg(KryoPoolSingleton.get().toBytesWithClass(message));
		ms.setCurrChunkStartIx(0);
		ms.hasSentAllChunks(false);
		// Tell the receiver's proxy it can start requesting chunks.
		this.log().debug("Sending start message for message with ID {}.", largeMessage.getId());
		this.sendMessageStates.put(largeMessage.getId(), ms);
		ms.getReceiverProxy().tell(new LargeMessageProxy.StartMessage(largeMessage.getId(), ms.getSerMsg().length), this.getSelf());

		// TODO: set a timeout.
	}

	private void handle(StartMessage startMessage) {
		final int messageId = startMessage.getId();
		this.log().debug("Handling start message for receiving a large message with ID {}.", messageId);
		// Check whether there is already a message with that particular ID.
		if (this.receiveMessageStates.containsKey(messageId)) {
			// TODO: throw;
		}

		ReceiveMessageState state = new ReceiveMessageState();
		state.setBuffer(new ByteArrayOutputStream(startMessage.getNumBytesSerialized()));
		state.setExpectedBytes(startMessage.getNumBytesSerialized());
		this.receiveMessageStates.put(messageId, state);

		this.log().debug("Have {} messages in progress ({}).", this.receiveMessageStates.size(), this.receiveMessageStates.keySet().stream().map((i) -> i.toString()).reduce("", String::concat));

		if (this.receiveMessageStates.size() < 3) {
			state.setStarted(true);
			this.log().debug("Requesting first block of message with ID {}.", messageId);
			sender().tell(new RequestNextChunkMessage(messageId), this.getSelf());
		} else {
			state.setStarted(false);
			this.log().debug("Deferring message with ID {}.", messageId);
		}
	}

	private void handle(RequestNextChunkMessage nextChunkMessage) {
		final int messageId = nextChunkMessage.getId();
		SendMessageState state = this.sendMessageStates.get(messageId);
		if (state == null) {
			// TODO: throw;
			return;
		}

		if (!state.hasSentAllChunks()) {
			// Get the next chunk and send it to the receiver's proxy.
			int currChunkEndIx = Math.min(state.getCurrChunkStartIx() + CHUNK_SIZE, state.getSerMsg().length);
			byte[] chunk = Arrays.copyOfRange(state.getSerMsg(), state.getCurrChunkStartIx(), currChunkEndIx);
			state.getReceiverProxy().tell(new NextChunkMessage(messageId, chunk), this.getSelf());

			if (currChunkEndIx == state.getSerMsg().length) {
				state.hasSentAllChunks(true);
				this.log().debug("All chunks sent for message with ID {}, sending NoChunksLeftMessage to receiver's proxy {}...", messageId, state.getReceiverProxy());
				state.getReceiverProxy().tell(
					new LargeMessageProxy.NoChunksLeftMessage(
						messageId,
						state.getSender(),
						state.getReceiver()
					),
					this.getSelf()
				);
				this.sendMessageStates.remove(messageId);
			} else {
				state.setCurrChunkStartIx(state.getCurrChunkStartIx() + CHUNK_SIZE);
			}
		} else {
			// TODO: Throw;
		}
	}

	// Receiver side...

	private void handle(NextChunkMessage message) throws IOException {
		final int messageId = message.getId();
		ReceiveMessageState state = this.receiveMessageStates.get(messageId);

		if (state == null) {
			// TODO: throw;
			return;
		}

		// Save the bytes from the received chunk and request the next one.
		state.getBuffer().write(message.getPayload());
		sender().tell(new RequestNextChunkMessage(messageId), this.getSelf());
	}

	private void handle(NoChunksLeftMessage noChunksLeftMessage) throws Exception {
		final int messageId = noChunksLeftMessage.getId();
		ReceiveMessageState state = this.receiveMessageStates.remove(messageId);

		if (state == null) {
			// TODO: throw;
			return;
		}

		this.log().debug("Received all chunks for large message with ID {}.", messageId);

		if (!state.hasSentDeserMsg()) {
			if (state.getBuffer().size() != state.expectedBytes) {
				throw new Exception(String.format(
					"Number of received bytes != number of bytes of serialized message [%d] != [%d]",
					state.getBuffer().size(),
					state.expectedBytes
				));
			}

			Object deserMsg = KryoPoolSingleton.get().fromBytes(state.getBuffer().toByteArray());
			this.log().debug("Deserialized message, sending it to actual receiver...");
			noChunksLeftMessage.getReceiver().tell(deserMsg, noChunksLeftMessage.getSender());
			state.hasSentDeserMsg(true);
		}

		// Start requesting packages for a message that hasn't been started.
		Entry<Integer, ReceiveMessageState> nextStartedState = this.receiveMessageStates.entrySet()
			.stream()
			.filter((s) -> !s.getValue().isStarted())
			.findFirst()
			.orElse(null);
		if (nextStartedState != null) {
			this.log().debug("Requesting deferred message with ID {}.", messageId);
			nextStartedState.getValue().setStarted(true);
			sender().tell(new RequestNextChunkMessage(nextStartedState.getKey()), this.getSelf());
		}
	}
}
