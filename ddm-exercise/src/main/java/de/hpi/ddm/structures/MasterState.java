package de.hpi.ddm.structures;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import akka.actor.ActorRef;
import de.hpi.ddm.utils.WorkItemComparator;
import lombok.Data;
import lombok.experimental.Accessors;

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

	private final TreeSet<WorkItem> workItems = new TreeSet<>(new WorkItemComparator());

	private int numHintsToCrack;  // num hints to crack before cracking a password

	@Accessors(fluent = true)
	private boolean hasInitializedNumHintsToCrack = false;

	private boolean anyWorkLeft  = true;

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

	public long getNumberOfUncrackedPasswords() {
		return this.getWorkItems().stream()
			.filter((item) -> !item.isCracked())
			.count();
	}

	public boolean hasUncrackedPasswords() {
		return this.getWorkItems().stream()
			.filter((item) -> !item.isCracked())
			.findFirst()
			.isPresent();
	}

	public boolean hasUnassignedWorkItems() {
		return this.getWorkItems().stream()
			.filter((item) -> !item.isCracked() && item.getWorkersCracking().isEmpty())
			.findFirst()
			.isPresent();
	}

	public WorkItem nextWorkItem() {
		if (this.getWorkItems().isEmpty()) {
			return null;
		}
		return this.getWorkItems().first();
	}

	WorkItem findWorkItemForPasswordId(int id) {
		return this.getWorkItems().stream()
			.filter((item) -> item.getPasswordEntry().getId() == id)
			.findFirst()
			.get();
	}

	public void addCrackingWorker(WorkItem item, ActorRef theCrackingWorker) throws NoSuchElementException, IllegalStateException {
		// Remove and add to trigger sortation.
		if (this.getWorkItems().remove(item) == false) {
			throw new NoSuchElementException("The given WorkItem was not found.");
		}

		item.getWorkersCracking().add(theCrackingWorker);
		// The work item should not be contained, as we removed it beforehand.
		if (!this.getWorkItems().add(item)) {
			throw new IllegalStateException("The item should not have been in the set of WorkItems.");
		}
	}

	public WorkItem removeCracked(int passwordId, ActorRef crackedBy) throws NoSuchElementException, IllegalStateException {
		final WorkItem crackedItem = this.findWorkItemForPasswordId(passwordId);
		if (crackedItem == null || !this.getWorkItems().remove(crackedItem)) {
			throw new NoSuchElementException("The WorkItem with password id " + passwordId + " was not found.");
		}

		crackedItem.setCracked(true);
		crackedItem.getWorkersCracking().remove(crackedBy);
		return crackedItem;
	}
}
