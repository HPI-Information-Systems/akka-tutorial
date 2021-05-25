package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
//		this.welcomeData = welcomeData;
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

	/**
	 * A message with the results of cracking a password for a password entry.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackedPasswordMessage implements Serializable {

		private static final long serialVersionUID = 17684700731281811L;
		private String result;  // concatenation of id + name + cracked password
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
//	private final BloomFilter welcomeData;

	private long startTime;


	// Our fields
	// ==========

	// TODO: change to a state object with getters/setters?

	private Queue<PasswordEntry> unassignedWork;
	private int numHintsToCrack;  // num hints to crack before cracking a password
	private boolean hasInitializedNumHintsToCrack;
	private boolean isAnyWorkLeft;
	private Set<ActorRef> busyWorkers;
	private Set<ActorRef> availableWorkers;
	private boolean isAlreadyAwaitingReadMessage;

	@Data
	static class PasswordEntry {
		private int id;
		private String name;
		private String passwordChars;
		private int passwordLength;
		private String passwordHash;
		private List<String> hintHashes;

		public PasswordEntry(String[] line) {  // TODO: replace with parseFromLine()?
			this.id = Integer.parseInt(line[0]);
			this.name = line[1];
			this.passwordChars = line[2];
			this.passwordLength = Integer.parseInt(line[3]);
			this.passwordHash = line[4];
			this.hintHashes = Arrays.stream(line, 5, line.length).collect(Collectors.toList());
		}
	}

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		unassignedWork = new LinkedList<>();
		hasInitializedNumHintsToCrack = false;
		isAnyWorkLeft = true;
		busyWorkers = new HashSet<>();
		availableWorkers = new HashSet<>();
		isAlreadyAwaitingReadMessage = false;
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(CrackedPasswordMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
		isAlreadyAwaitingReadMessage = true;
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.availableWorkers.add(this.sender());
		this.workers.add(this.sender());  // TODO: do we need `workers`? Maybe only use the sets with available and busy workers?
		this.log().info("Registered {}", this.sender());

		// TODO: do we really need to send a welcome message? Maybe remove it altogether?
//		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());

		if (!isAnyWorkLeft && !unassignedWork.isEmpty()) {
			System.out.println("NO WORK LEFT");
			return;
		}

		// Assign some work to registering workers. Note that the processing of the global task might have already started.
		if (!unassignedWork.isEmpty()) {
			// Give the new worker some work.
			this.busyWorkers.add(this.sender());
			this.sender().tell(new Worker.WorkMessage(unassignedWork.remove(), numHintsToCrack), this.getSelf());
		} else {  // no unassigned work but there's some work left
			this.availableWorkers.add(this.sender());
			if (!isAlreadyAwaitingReadMessage) {
				this.reader.tell(new Reader.ReadMessage(), this.getSelf());
				isAlreadyAwaitingReadMessage = true;
			}
		}
	}

	protected void handle(BatchMessage message) {

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

		this.log().warning("Received BatchMessage from " + this.getSender());

		isAlreadyAwaitingReadMessage = false;

		// Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		if (message.getLines().isEmpty()) {
			if (!busyWorkers.isEmpty()) {
				this.log().warning("No more work left");
				isAnyWorkLeft = false;
			} else {
				// This can happen if all our workers (e.g., 2 workers) finish cracking their last passwords approx.
				// at the same time. In this case, since we remove workers from `busyWorkers` when a `CrackedPasswordMessage`
				// arrives and there is no work in `unassignedWork` left and, if `isAnyWorkLeft is `true`, ask the reader
				// to send the next `BatchMessage`, it might occur that all `CrackedPasswordMessage`s arrive nearly at the
				// same time, before the `BatchMessage` with no lines reaches the master. A similar check is performed in the
				// `handle(CrackedPasswordMessage crackedPasswordMessage)` method, in its turn, it will trigger termination
				// when an empty `BatchMessage` arrives from the reader before one of the `CrackedPasswordMessage`s with
				// the last cracked passwords.
				this.terminate();
			}
			return;
		}

		unassignedWork.addAll(message.getLines().stream().map(PasswordEntry::new).collect(Collectors.toList()));

		if (!hasInitializedNumHintsToCrack) {
			// Work out number of hints to crack before cracking the password, this needs to be done only once since
			// all password entries follow the same pattern (i.e., they feature the same password length, possible chars
			// (a.k.a. alphabet chars a.k.a. passwordChars), number of hints, etc.).
			// TODO: implement the algorithm from the "Cracking estimation difficulty.pdf" file
//			PasswordEntry passwordEntry = unassignedWork.peek();
			numHintsToCrack = 0;
			hasInitializedNumHintsToCrack = true;
			this.log().warning("Workers will crack {} hints before cracking a password", numHintsToCrack);
		}

		// Send the unassigned work to the available workers (1 password entry per available worker).
		Queue<ActorRef> availableWorkersQueue = new LinkedList<>(availableWorkers);
		while (!availableWorkersQueue.isEmpty() && !unassignedWork.isEmpty()) {
			PasswordEntry passwordEntry = unassignedWork.remove();  // poll() can be used as well
			ActorRef availableWorker = availableWorkersQueue.remove();
			availableWorker.tell(new Worker.WorkMessage(passwordEntry, numHintsToCrack), this.getSelf());
			busyWorkers.add(availableWorker);
			availableWorkers.remove(availableWorker);
		}

		if (!availableWorkersQueue.isEmpty()) {  // if some available workers are left with no work
			// Get more work from the reader.
			this.sender().tell(new Reader.ReadMessage(), this.getSelf());
			isAlreadyAwaitingReadMessage = true;
		}  // else: if no free workers are available, but there is still unassigned work left, that's fine,
		// the workers finished with their jobs will receive such work as a reply to `CrackedPasswordMessage`.
	}

	private void handle(CrackedPasswordMessage crackedPasswordMessage) {
		// Send partial results to the collector.
		this.log().warning("Received result {}, sending result to collector...", crackedPasswordMessage.getResult());
		collector.tell(new Collector.CollectMessage(crackedPasswordMessage.getResult()), this.self());

		if (isAnyWorkLeft) {
			if (!unassignedWork.isEmpty()) {
				// Send some unassigned work to the worker.
				PasswordEntry passwordEntry = unassignedWork.remove();  // `poll()` can be used as well
				this.sender().tell(new Worker.WorkMessage(passwordEntry, numHintsToCrack), this.getSelf());
			} else {
				busyWorkers.remove(this.sender());  // the worker will become busy again when/if it gets assigned some work in `handle(BatchMessage message)`
				availableWorkers.add(this.sender());
				// Request more work from the reader.
				if (!isAlreadyAwaitingReadMessage) {
					this.reader.tell(new Reader.ReadMessage(), getSelf());
					isAlreadyAwaitingReadMessage = true;
				}
			}
		} else {
			if (!unassignedWork.isEmpty()) {
				// Send some unassigned work to the worker.
				PasswordEntry passwordEntry = unassignedWork.remove();
				this.sender().tell(new Worker.WorkMessage(passwordEntry, numHintsToCrack), this.getSelf());
			} else {
				busyWorkers.remove(sender());
				this.log().warning("No work left, removed worker {}, busy workers left = {}", this.sender(), busyWorkers.size());

				if (busyWorkers.isEmpty()) {
					this.log().warning("Neither any work nor busy workers left, terminating...");
					terminate();
				}
			}
		}
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
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
}
