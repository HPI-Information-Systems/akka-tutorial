package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;

	private long startTime;

	private int totalNumberOfPermutationTasks = 0;
	private int totalNumberOfProcessedPermutationTasks = 0;
	private boolean finishedReadingPasswordCharsSubsets = false;
	private int totalNumberOfPermutations = 0;
	private int totalNumberOfCalculatedHashes = 0;

	private boolean finishedStartingCalculationOfPermutationHashed = false;

	private boolean finishedCalculatingPermutationHashes = false;
	private List<String[]> records = new ArrayList<>();

	private int totalNumberOfResolvedRecords = 0;

	private int totalNumberOfRecords = 0;
	private int totalNumberOfProcessedRecords = 0;
	private boolean finishedReadingRecords = false;

	private int workerIdx = 0;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReceivePermutationsMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609588L;
		private List<String> permutations;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReceivePermutationHashMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609588L;
		private List<Worker.ResolvedHint> resolvedHints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReceiveResolvedRecordMessage implements Serializable {
		private static final long serialVersionUID = 8344040942748609588L;
		private String record;
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(String.class, this::handle)
				.match(ReceivePermutationsMessage.class, this::handle)
				.match(ReceivePermutationHashMessage.class, this::handle)
				.match(ReceiveResolvedRecordMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(ReceiveResolvedRecordMessage message) {
		System.out.println("ReceiveResolvedRecordMessage message from worker " + message);
		this.collector.tell(new Collector.CollectMessage(message.getRecord()), this.self());
		totalNumberOfResolvedRecords++;
		if(totalNumberOfResolvedRecords == totalNumberOfRecords){
			terminate();
		}
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private static <T> Stream<List<T>> batches(List<T> source, int length) {
		if (length <= 0)
			throw new IllegalArgumentException("length = " + length);
		int size = source.size();
		if (size <= 0)
			return Stream.empty();
		int fullChunks = (size - 1) / length;
		return IntStream.range(0, fullChunks + 1).mapToObj(
				n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
	}


	protected void handle(ReceivePermutationsMessage message) {
		System.out.println("ReceivePermutationsMessage from worker " + message.getPermutations().size());
		totalNumberOfProcessedPermutationTasks++;
		totalNumberOfPermutations += message.getPermutations().size();

		if(finishedReadingPasswordCharsSubsets && totalNumberOfProcessedPermutationTasks == totalNumberOfPermutationTasks) {
			finishedStartingCalculationOfPermutationHashed= true;
		}
		batches(message.getPermutations(), 100000).forEach(permutationsBatch -> {
			this.workers.get(workerIdx).tell(new Worker.BuildPermutationHashMessage(permutationsBatch, records), this.self());
			workerIdx = (workerIdx + 1) % this.workers.size();
		});
	}

	protected void handle(ReceivePermutationHashMessage message) {
		List<Worker.ResolvedHint> resolvedHints = message.resolvedHints;
		resolvedHints.forEach(hint -> {
			String[] record = records.get(hint.getRow());
			record[hint.getColumn()] = hint.getResolvedValue();
			records.set(hint.getRow(), record);
		});
		totalNumberOfCalculatedHashes++;
		if(finishedStartingCalculationOfPermutationHashed && totalNumberOfCalculatedHashes == totalNumberOfPermutations) {
			finishedCalculatingPermutationHashes = true;
			records.forEach(record -> {
				this.workers.get(workerIdx).tell(new Worker.CrackPasswordMessage(record), this.self());
				workerIdx = (workerIdx + 1) % this.workers.size();
			});
		}
	}

	protected void handle(BatchMessage message) {
		System.out.println("handle master " + message.getLines().size());
		System.out.println(message);
		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		String passwordChars = null;

		if (message.getLines().isEmpty()) {
			finishedReadingRecords = true;
		} else {
			if(passwordChars == null) {
				passwordChars = message.getLines().get(0)[2];
				for(int i = 0; i < passwordChars.length() ; i++){
					String passwordCharsSubset = new StringBuilder(passwordChars).deleteCharAt(i).toString();
					this.workers.get(workerIdx).tell(new Worker.BuildPermutationsMessage(passwordCharsSubset), this.self());
					workerIdx = (workerIdx + 1) % this.workers.size();
					totalNumberOfPermutationTasks++;
				}
				finishedReadingPasswordCharsSubsets = true;
			}
			totalNumberOfRecords += message.getLines().size();
			records.addAll(message.getLines());
			this.reader.tell(new Reader.ReadMessage(), this.self());
		}
	}
	
	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());
		
		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());
		
		// TODO: Assign some work to registering workers. Note that the processing of the global task might have already started.
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}

	protected void handle(String message) {
		totalNumberOfProcessedRecords++;
		System.out.println("processed data from worker " + message);
		if(finishedReadingRecords && totalNumberOfRecords == totalNumberOfProcessedRecords){
			this.terminate();
		}
	}

}
