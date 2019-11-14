package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import de.hpi.ddm.serialization.Chunk;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	private static final int KB = 1024;
	private static final int MB = KB * KB;
	private static final int CHUNK_SIZE_BYTES = MB;
	private static final int BUFFER_SIZE = 5;

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
		private static final long serialVersionUID = -4984077707340712941L;
		private T bytes;

	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class StreamCompleted implements Serializable {
		private static final long serialVersionUID = 2546055321597800174L;
		private ActorRef sender;
		private ActorRef receiver;
		private int chunkCount;
	}

	@Data @AllArgsConstructor
	public static class StreamFailure implements Serializable {
		private static final long serialVersionUID = 4593304793354806849L;
		private final Throwable cause;
	}

	/////////////////
	// Actor State //
	/////////////////

	private List<Chunk> messageChunks = new LinkedList<>();
	
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
				.match(StreamCompleted.class, this::handle)
				.match(StreamFailure.class, this::handle)
				.match(Chunk.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		Duration timeout = Duration.ofSeconds(5);
		ActorRef receiverProxy;

		try {
			receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME))
					.resolveOne(timeout).toCompletableFuture().get();
		} catch (InterruptedException | ExecutionException e) {
			log().error("Could not connect to ReceiverProxy.");
			e.printStackTrace();
			return;
		}

		BytesMessage<?> bytesMessage = new BytesMessage<>(message.getMessage());
		List<Chunk> messageContents = serializeAndSplit(bytesMessage);

		Source<Chunk, NotUsed> source = Source.from(messageContents);
		Sink<Chunk, NotUsed> sink = Sink.actorRef(
				receiverProxy,
				new StreamCompleted(this.sender(), receiver, messageContents.size()));
		source.buffer(BUFFER_SIZE, OverflowStrategy.backpressure())
				.runWith(sink, ActorMaterializer.create(this.context().system()));
	}

	private void handle(Chunk messageChunk) {
		this.messageChunks.add(messageChunk);
	}

	private void handle(StreamCompleted completed) {
		log().info("Stream completed with {} chunks", this.messageChunks.size());

		if (messageChunks.size() < completed.chunkCount) {
			log().error("Did not receive all message chunks of LargeMessage to {}", completed.receiver);
			discardMessageChunks();
			return;
		}
		BytesMessage<?> message = null;
		try {
			message = joinAndDeserialize(this.messageChunks);
		} catch (IOException | KryoException | IndexOutOfBoundsException e) {
			log().error("Could not deserialize LargeMessage to {}", completed.receiver);
		} finally {
			discardMessageChunks();
		}

		if (message != null) {
			completed.receiver.tell(message.bytes, completed.sender);
			log().info("Sent LargeMessage of {} to {}", message.bytes.getClass(), completed.receiver);
		}
	}

	private void handle(StreamFailure failed) {
		discardMessageChunks();
		log().error(failed.getCause(), "Stream failed");
	}

	private <T> List<Chunk> serializeAndSplit(BytesMessage<T> message) {
		return split(serialize(message));
	}
	private static BytesMessage<?> joinAndDeserialize(List<Chunk> data) throws IOException {
		return deserialize(join(data));
	}

	private static <T> byte[] serialize(BytesMessage<T> message) {
		Kryo kryo = getKryo();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		Output output = new Output(stream);
		kryo.writeObject(output, message);
		output.close();
		byte[] buffer = stream.toByteArray();

		try {
			stream.close();
		} catch (IOException e) {
			// ignore
		}
		return buffer;
	}

	private static List<Chunk> split(byte[] bytes) {
		List<Chunk> result = new LinkedList<>();
		for(int i = 0; i < bytes.length; i += CHUNK_SIZE_BYTES) {
			int end = Math.min(i + CHUNK_SIZE_BYTES, bytes.length);
			result.add(new Chunk(Arrays.copyOfRange(bytes, i, end)));
		}
		return result;
	}

	private static byte[] join(List<Chunk> bytes) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buffer;
		try {
			for(Chunk i: bytes) {
				out.write(i.getBytes());
			}
			buffer = out.toByteArray();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				// ignore
			}
		}
		return buffer;
	}

	private static BytesMessage<?> deserialize(byte[] data)  {
		Kryo kryo = getKryo();
		return kryo.readObject(new Input(new ByteArrayInputStream(data)),
				BytesMessage.class);
	}

	private static Kryo getKryo() {
		Kryo kryo = new Kryo();
		FieldSerializer<?> serializer = new FieldSerializer<BytesMessage<?>>(kryo, BytesMessage.class);
		kryo.register(BytesMessage.class, serializer);
		return kryo;
	}

	private void discardMessageChunks() {
		this.messageChunks = new LinkedList<>();
	}
}
