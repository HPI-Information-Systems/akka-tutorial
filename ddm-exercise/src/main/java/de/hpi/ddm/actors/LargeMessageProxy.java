package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

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
	public static final int CHUNK_SIZE = 512;
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	// Messages to be sent to the sender's proxy
	// =========================================

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	/**
	 * A message to be sent to the sender's proxy to ask it to send the next chunk to the receiver's proxy.
	 */
	@Data
	public static class RequestNextChunkMessage implements Serializable {
		private static final long serialVersionUID = -5374303325056263165L;
	}

	// Messages to be sent to the receiver's proxy
	// ===========================================

	/**
	 * A message to be sent to the receiver's proxy to let it know it can start asking the sender's proxy
	 * for the first chunk.
	 */
	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = 2746268654101845915L;
	}

	/**
	 * A message containing the next chunk.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class NextChunkMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private byte[] bytes;
	}

	/**
	 * A message to be sent to the receiver's proxy to let it know that no more chunks are left. The fields of this
	 * message contain the actual sender and receiver (i.e., not the proxies) which are used for forwarding the
	 * reassembled and deserialized message to the actual receiver. Also, this message contains the number of bytes
	 * of the serialized message so that the receiver's proxy can verify it has received all the bytes.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class NoChunksLeftMessage implements Serializable {
		private static final long serialVersionUID = -4373062859625501319L;
		private ActorRef sender;
		private ActorRef receiver;
		private int numBytesSer;
	}

	/////////////////
	// Actor State //
	/////////////////

	// Fields of the sender's proxy
	private byte[] serMsg;
	private ActorRef sender, receiver;
	private ActorSelection receiverProxy;
	private int currChunkStartIx;
	private boolean hasSentAllChunks;

	// Fields of the receiver's proxy
	private ByteArrayOutputStream baos;  // used for storing the bytes from the received chunks
	private boolean hasSentDeserMsg;

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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
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
		this.log().info("Received large message.");
		Object message = largeMessage.getMessage();
		// Save the references to the actual sender and receiver, as well as to the receiver's proxy into the fields
		// of the sender's proxy.
		this.sender = this.sender();
		this.receiver = largeMessage.getReceiver();
		this.receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// Serialize the message and initialize/reset the sender's proxy fields.
		this.serMsg = KryoPoolSingleton.get().toBytesWithClass(message);
		this.currChunkStartIx = 0;
		this.hasSentAllChunks = false;
		// Tell the receiver's proxy it can start requesting chunks.
		this.receiverProxy.tell(new LargeMessageProxy.StartMessage(), this.getSelf());
	}

	private void handle(StartMessage startMessage) {
		this.baos = new ByteArrayOutputStream();
		sender().tell(new RequestNextChunkMessage(), this.getSelf());
	}

	private void handle(RequestNextChunkMessage nextChunkMessage) {
		if (!this.hasSentAllChunks) {
			// Get the next chunk and send it to the receiver's proxy.
			int currChunkEndIx = Math.min(currChunkStartIx + CHUNK_SIZE, serMsg.length);
			byte[] chunk = Arrays.copyOfRange(serMsg, currChunkStartIx, currChunkEndIx);
			receiverProxy.tell(new NextChunkMessage(chunk), this.getSelf());

			if (currChunkEndIx == this.serMsg.length) {
				this.hasSentAllChunks = true;
				this.log().info("All chunks sent, sending NoChunksLeftMessage to receiver's proxy {}...", receiverProxy);
				receiverProxy.tell(new LargeMessageProxy.NoChunksLeftMessage(this.sender, this.receiver, this.serMsg.length), this.getSelf());
			} else {
				currChunkStartIx += CHUNK_SIZE;
			}
		}
	}

	private void handle(NextChunkMessage message) throws IOException {
		// Save the bytes from the received chunk and request the next one.
		baos.write(message.getBytes());
		sender().tell(new RequestNextChunkMessage(), this.getSelf());
	}

	private void handle(NoChunksLeftMessage noChunksLeftMessage) throws Exception {
		if (!hasSentDeserMsg) {
			byte[] serMsg = baos.toByteArray();  // technically, `this.serMsg` also could be used here since its unset for the receiver's proxy
			if (serMsg.length != noChunksLeftMessage.getNumBytesSer()) {
				throw new Exception(String.format("Number of received bytes != number of bytes of serialized message [%d] != [%d]", serMsg.length, noChunksLeftMessage.numBytesSer));
			}

			Object deserMsg = KryoPoolSingleton.get().fromBytes(serMsg);
			this.log().info("Deserialized message, sending it to actual receiver...");
			noChunksLeftMessage.getReceiver().tell(deserMsg, noChunksLeftMessage.getSender());
			hasSentDeserMsg = true;
		}
	}
}
