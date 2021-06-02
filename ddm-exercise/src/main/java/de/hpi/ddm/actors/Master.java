package de.hpi.ddm.actors;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.structures.MasterState;
import de.hpi.ddm.structures.PasswordEntry;
public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		ActorRef largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.state = new MasterState(reader, collector, largeMessageProxy, welcomeData);

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

	// State object holding all the state. :)
	final MasterState state;

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
			.match(StartMessage.class, this::handle)
			.match(RegistrationMessage.class, this::handle)
			.match(BatchMessage.class, this::handle)
			.match(CrackedPasswordMessage.class, this::handle)
			.match(Terminated.class, this::handle)
			.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
			.build();
	}

	protected void handle(StartMessage message) {
		this.state.setStartTime(System.currentTimeMillis());

		this.state.getReader().tell(new Reader.ReadMessage(), this.self());
		this.state.setAlreadyAwaitingReadMessage(true);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.state.getAvailableWorkers().add(this.sender());
		this.state.getWorkers().add(this.sender());  // TODO: do we need `workers`? Maybe only use the sets with available and busy workers?
		this.log().info("Registered {}", this.sender());

		// TODO: do we really need to send a welcome message? Maybe remove it altogether?
		this.state.getLargeMessageProxy().tell(
			new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.state.getWelcomeData()), this.sender()
		), this.self());

		if (!this.state.isAnyWorkLeft() && !this.state.getUnassignedWork().isEmpty()) {
			this.log().warning("NO WORK LEFT");
			return;
		}

		// Assign some work to registering workers. Note that the processing of the global task might have already started.
		if (!this.state.getUnassignedWork().isEmpty()) {
			// Give the new worker some work.
			this.state.getBusyWorkers().add(this.sender());
			this.state.getLargeMessageProxy().tell(new LargeMessageProxy.LargeMessage<>(
				new Worker.WorkMessage(this.state.getUnassignedWork().remove(), this.state.getNumHintsToCrack()), this.sender()
			), this.getSelf());
		} else {
			// no unassigned work but there's some work left
			this.state.getAvailableWorkers().add(this.sender());
			if (!this.state.isAlreadyAwaitingReadMessage()) {
				this.state.getReader().tell(new Reader.ReadMessage(), this.getSelf());
				this.state.setAlreadyAwaitingReadMessage(true);
			}
		}
	}

	protected void handle(BatchMessage message) {
		this.log().info("Received BatchMessage from " + this.getSender());

		this.state.setAlreadyAwaitingReadMessage(false);

		// Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then.
		if (message.getLines().isEmpty()) {
			if (!this.state.getBusyWorkers().isEmpty()) {
				this.log().info("No more work left");
				this.state.setAnyWorkLeft(false);
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

		this.state.getUnassignedWork().addAll(message.getLines().stream().map(PasswordEntry::parseFromLine).collect(Collectors.toList()));

		if (!this.state.hasInitializedNumHintsToCrack()) {
			// Work out number of hints to crack before cracking the password, this needs to be done only once since
			// all password entries follow the same pattern (i.e., they feature the same password length, possible chars
			// (a.k.a. alphabet chars a.k.a. passwordChars), number of hints, etc.).
			// TODO: implement the algorithm from the "Cracking estimation difficulty.pdf" file
			PasswordEntry passwordEntry = this.state.getUnassignedWork().peek();
			this.state.setNumHintsToCrack(
				getNumHintsToCrack(
					passwordEntry.getPasswordChars().length(),
					 passwordEntry.getHintHashes().size(),
					passwordEntry.getPasswordLength()
				)
			);
			this.state.hasInitializedNumHintsToCrack(true);
			this.log().info("Workers will crack {} hint(s) before cracking password", this.state.getNumHintsToCrack());
		}

		// Send the unassigned work to the available workers (1 password entry per available worker).
		Queue<ActorRef> availableWorkersQueue = new LinkedList<>(this.state.getAvailableWorkers());
		while (!availableWorkersQueue.isEmpty() && !this.state.getUnassignedWork().isEmpty()) {
			PasswordEntry passwordEntry = this.state.getUnassignedWork().remove();  // poll() can be used as well
			ActorRef availableWorker = availableWorkersQueue.remove();
			this.state.getLargeMessageProxy().tell(new LargeMessageProxy.LargeMessage<>(
				new Worker.WorkMessage(passwordEntry, this.state.getNumHintsToCrack()), availableWorker
			), this.getSelf());
			this.state.getAvailableWorkers().remove(availableWorker);
			this.state.getBusyWorkers().add(availableWorker);
		}

		if (!availableWorkersQueue.isEmpty()) {  // if some available workers are left with no work
			// Get more work from the reader.
			this.sender().tell(new Reader.ReadMessage(), this.getSelf());
			this.state.setAlreadyAwaitingReadMessage(true);
		}  // else: if no free workers are available, but there is still unassigned work left, that's fine,
		// the workers finished with their jobs will receive such work as a reply to `CrackedPasswordMessage`.
	}

	private void handle(CrackedPasswordMessage crackedPasswordMessage) {
		// Send partial results to the collector.
		this.log().info("Received result {}, sending result to collector...", crackedPasswordMessage.getResult());
		this.state.getCollector().tell(new Collector.CollectMessage(crackedPasswordMessage.getResult()), this.self());

		if (this.state.isAnyWorkLeft()) {
			if (!this.state.getUnassignedWork().isEmpty()) {
				// Send some unassigned work to the worker.
				this.state.getLargeMessageProxy().tell(new LargeMessageProxy.LargeMessage<>(
						new Worker.WorkMessage(this.state.getUnassignedWork().remove(), this.state.getNumHintsToCrack()), this.sender()
				), this.getSelf());
			} else {
				this.state.getBusyWorkers().remove(this.sender());  // the worker will become busy again when/if it gets assigned some work in `handle(BatchMessage message)`
				this.state.getAvailableWorkers().add(this.sender());
				// Request more work from the reader.
				if (!this.state.isAlreadyAwaitingReadMessage()) {
					this.state.getReader().tell(new Reader.ReadMessage(), getSelf());
					this.state.setAlreadyAwaitingReadMessage(true);
				}
			}
		} else {
			if (!this.state.getUnassignedWork().isEmpty()) {
				// Send some unassigned work to the worker.
				this.state.getLargeMessageProxy().tell(new LargeMessageProxy.LargeMessage<>(
					new Worker.WorkMessage(this.state.getUnassignedWork().remove(), this.state.getNumHintsToCrack()), this.sender()
				), this.getSelf());
			} else {
				this.state.getBusyWorkers().remove(sender());
				this.log().info("No work left, removed worker {}, busy workers left = {}", this.sender(), this.state.getBusyWorkers().size());

				if (this.state.getBusyWorkers().isEmpty()) {
					this.log().info("Neither any work nor busy workers left, terminating...");
					terminate();
				}
			}
		}
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.state.getWorkers().remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}

	protected void terminate() {
		this.state.getCollector().tell(new Collector.PrintMessage(), this.self());

		this.state.getReader().tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.state.getCollector().tell(PoisonPill.getInstance(), ActorRef.noSender());

		for (ActorRef worker : this.state.getWorkers()) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}

		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

		long executionTime = System.currentTimeMillis() - this.state.getStartTime();
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	private int getNumHintsToCrack(int numAlphabetChars, int numHints, int passwordLength) {
		int numUniquePasswordChars = numAlphabetChars - numHints;
		for (int numCrackedHints = 0; numCrackedHints < numHints; numCrackedHints++) {
			BigInteger nextHintDifficulty = BigInteger.valueOf(numAlphabetChars - numCrackedHints).multiply(factorial(numAlphabetChars - 1));
			BigInteger passwordDifficulty = factorial(numAlphabetChars - numCrackedHints).divide(
					factorial(numUniquePasswordChars).multiply(factorial(numHints - numCrackedHints))).multiply(BigInteger.valueOf((long) Math.pow(numUniquePasswordChars, passwordLength)));
			BigInteger passwordDifficultyAfterNextHint = nextHintDifficulty.add(factorial(numAlphabetChars - numCrackedHints - 1).divide(
					factorial(numUniquePasswordChars).multiply(factorial(numHints - numCrackedHints - 1))).multiply(BigInteger.valueOf((long) Math.pow(numUniquePasswordChars, passwordLength))));
			if (passwordDifficulty.compareTo(passwordDifficultyAfterNextHint) < 0) {
				return numCrackedHints;
			}
		}
		return numHints;
	}

	/**
	 * Use BigInteger to be able to compute the factorial for numbers over 20.
	 */
	private BigInteger factorial(int n) {
		BigInteger result = BigInteger.ONE;
		for (int i = 2; i <= n; i++)
			result = result.multiply(BigInteger.valueOf(i));
		return result;
	}
}
