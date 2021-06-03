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
import de.hpi.ddm.structures.WorkItem;

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
		private int id;
		private String name;
		private String crackedPassword;
	}

	@Data
	public static class GetNextWorkItemMessage implements Serializable {
		private static final long serialVersionUID = 3820580630641612339L;
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
			.match(GetNextWorkItemMessage.class, this::handle)
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
		this.state.getWorkers().add(this.sender());
		this.log().info("Registered {}", this.sender());

		this.state.getLargeMessageProxy().tell(
			new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.state.getWelcomeData()), this.sender()
		), this.self());
	}

	protected void handle(BatchMessage message) {
		this.log().info("Received BatchMessage from " + this.getSender());

		this.state.setAlreadyAwaitingReadMessage(false);

		// Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then.
		if (message.getLines().isEmpty()) {
			this.log().info("No more work left in the password file.");
			this.state.setAnyWorkLeft(false);
			if (!this.state.hasUncrackedPasswords() && !this.state.hasUnassignedWorkItems()) {  // TODO: is it possible that one predicate here will be true and the other false?
				this.log().info("No unassigned work items left, terminating....");
				terminate();
			}
			return;
		}

		this.state.getWorkItems().addAll(
			message.getLines().stream()
				.map(PasswordEntry::parseFromLine)
				.map((passwordEntry) -> new WorkItem(passwordEntry))
				.collect(Collectors.toList())
		);

		if (!this.state.hasInitializedNumHintsToCrack()) {
			// Work out number of hints to crack before cracking the password, this needs to be done only once since
			// all password entries follow the same pattern (i.e., they feature the same password length, possible chars
			// (a.k.a. alphabet chars a.k.a. passwordChars), number of hints, etc.).
			PasswordEntry passwordEntry = this.state.getWorkItems().first().getPasswordEntry();
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
	}

	protected void handle(GetNextWorkItemMessage message) {
		// Assign some work to workers. Note that the processing of the global task might have already started.
		if (!this.state.hasUnassignedWorkItems() && this.state.isAnyWorkLeft()) {
			// No unassigned work left, but there's still some work left on the reader's side.
			if (!this.state.isAlreadyAwaitingReadMessage()) {
				this.state.getReader().tell(new Reader.ReadMessage(), this.self());
				this.state.setAlreadyAwaitingReadMessage(true);
			}
			// Send a message that currently there is no work and the worker should try again later.
			this.sender().tell(new Worker.WorkMessage(), this.self());
			return;
		}

		final WorkItem nextWorkItem = this.state.nextWorkItem();

		if (nextWorkItem == null) {
			// Send a message, that currently there is no work and the worker should try again later.
			this.sender().tell(new Worker.WorkMessage(), this.self());
			return;
		}

		final boolean shouldRandomize = !nextWorkItem.getWorkersCracking().isEmpty();
		this.state.addCrackingWorker(nextWorkItem, this.sender());

		if (shouldRandomize) {
			this.log().info(
				"Straggler Avoidance: Sending already in progress password {} of {} to {}.",
				nextWorkItem.getPasswordEntry().getId(),
				nextWorkItem.getPasswordEntry().getName(),
				this.sender().toString()
			);
		} else {
			this.log().info(
				"Sending password {} of {} to {}.",
				nextWorkItem.getPasswordEntry().getId(),
				nextWorkItem.getPasswordEntry().getName(),
				this.sender().toString()
			);
		}
	
		this.state.getLargeMessageProxy().tell(new LargeMessageProxy.LargeMessage<>(
			new Worker.WorkMessage(nextWorkItem.getPasswordEntry(), this.state.getNumHintsToCrack(), shouldRandomize),
			this.sender()
		), this.self());
	}

	private void handle(CrackedPasswordMessage crackedPasswordMessage) {
		// Send partial results to the collector.
		final String result = crackedPasswordMessage.getId() + " " + crackedPasswordMessage.getName() + " " + crackedPasswordMessage.getCrackedPassword();

		this.log().info(
			"Received result {} from {}, sending result to collector...",
			result,
			this.sender()
		);
		this.state.getCollector().tell(new Collector.CollectMessage(result), this.self());

		try {
			WorkItem crackedItem = this.state.removeCracked(crackedPasswordMessage.getId(), this.sender());
			for (ActorRef workingActor : crackedItem.getWorkersCracking()) {
				this.log().info("Telling {} to stop current cracking.", workingActor);
				workingActor.tell(new Worker.StopCrackMessage(), this.self());
			}
		} catch (NoSuchElementException ex) {
			this.log().info("Password {} was already cracked.", crackedPasswordMessage.getId());
		}

		// We are very polite.
		this.sender().tell(new Worker.ThanksForCrackMessage(), this.self());

		// Everything done.
		if (!this.state.hasUncrackedPasswords()) {
			if (!this.state.isAnyWorkLeft()) {
				this.log().info("All passwords cracked. Terminating.");
				terminate();
			} else {
				// Request the next batch of lines from the reader if no one has already done so.
				if (!this.state.isAlreadyAwaitingReadMessage()) {
					this.state.getReader().tell(new Reader.ReadMessage(), this.getSelf());
					this.state.setAlreadyAwaitingReadMessage(true);
				}
			}
		} else {
			this.log().info("{} passwords left to crack in batch.", this.state.getNumberOfUncrackedPasswords());
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
