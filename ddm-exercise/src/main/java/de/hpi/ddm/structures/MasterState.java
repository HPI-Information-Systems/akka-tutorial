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

	private final Queue<WorkItem> unassignedWorkItems = new LinkedList<>();
	private final List<WorkItem> assignedWorkItems = new LinkedList<>();
	private int numHintsToCrack;  // num hints to crack before cracking a password

	@Accessors(fluent = true)
	private boolean hasInitializedNumHintsToCrack = false;

	private boolean anyWorkLeft  = true;

	private final Set<ActorRef> busyWorkers = new HashSet<>();

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

	//////////////////
	// Retrieval
	//////////////////

	public boolean hasUnassignedWorkItems() {
		return !this.getUnassignedWorkItems().isEmpty();
	}

	public boolean hasUncrackedPasswords() {
		return this.unassignedWorkItems.stream().filter((item) -> !item.isCracked()).findFirst().isPresent();
	}

	public WorkItem findWorkItemForPasswordId(int id) {
		return this.getAssignedWorkItems().stream()
			.filter((item) -> item.getPasswordEntry().getId() == id)
			.findFirst()
			.get();
	}
}
