package de.hpi.ddm.structures;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import akka.actor.ActorRef;
import lombok.experimental.Accessors;
import lombok.Data;

@Data
public class MasterState {
	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers = new LinkedList<>();
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;

	private long startTime;


	// Our fields
	// ==========

	private final Queue<PasswordEntry> unassignedWork = new LinkedList<>();
	private int numHintsToCrack;  // num hints to crack before cracking a password

	@Accessors(fluent = true)
	private boolean hasInitializedNumHintsToCrack = false;

	private boolean anyWorkLeft  = true;

	private final Set<ActorRef> busyWorkers = new HashSet<>();

	private final Set<ActorRef> availableWorkers = new HashSet<>();

	private boolean alreadyAwaitingReadMessage = false;

	//////////////////
	// Initializer
	//////////////////

	public MasterState(ActorRef reader, ActorRef collector, ActorRef largeMessageProxy, BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.largeMessageProxy = largeMessageProxy;
		this.welcomeData = welcomeData;
	}
}
