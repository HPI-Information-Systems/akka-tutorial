package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.CharSetManager;
import de.hpi.ddm.structures.Hint;
import de.hpi.ddm.structures.Person;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.waitingWorkers = new HashSet<>();
		this.completedPersons = new HashSet<>();
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

	@Data
	public static class WorkRequest implements Serializable {
		private static final long serialVersionUID = 4733690207607478879L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ExcludedChar implements Serializable {
		private static final long serialVersionUID = -2056262088677876779L;
		private Integer personID;
		private char value;
		private String hash;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class IncludedChar implements Serializable {
		private static final long serialVersionUID = 2424969000764642610L;
		private Integer personID;
		private char value;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class Solution implements Serializable {
		private static final long serialVersionUID = -2964092903137759190L;
		private Integer personID;
		private String solution;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final Set<Integer> completedPersons;
	private int batchSize;
	private Set<ActorRef> waitingWorkers;

	private long startTime;
	private Map<Integer, Person> persons;
	private CharSetManager charSetManager;

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
				.match(BatchMessage.class, this::handle)
				.match(WorkRequest.class, this::handle)
				.match(ExcludedChar.class, this::handle)
				.match(IncludedChar.class, this::handle)
				.match(Solution.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}


	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(BatchMessage message) {
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		this.batchSize = message.getLines().size();
		this.charSetManager = CharSetManager.fromMessageLine(message.lines.get(0), this.batchSize);
		this.persons = parseLines(message.lines);
		distributeWork();
	}

	private void handle(WorkRequest workRequest) {
		if (this.persons == null || this.persons.size() == 0) {
			this.waitingWorkers.add(this.sender());
			return;
		}

		sendWorkItem(this.sender());
		distributeWork();
	}

	private void handle(ExcludedChar excludedChar) {
		if (!this.completedPersons.contains(excludedChar.personID)) {
			this.charSetManager.handleExcludedChar(excludedChar.value, excludedChar.personID, excludedChar.hash);
			Person person = this.persons.get(excludedChar.personID);
			person.dropChar(excludedChar.value);
			for (Hint hint : person.getHints()) {
				if(hint.getValue().equals(excludedChar.hash)) {
					person.getHints().remove(hint);
					return;
				}
			}
		}
	}

	private void handle(IncludedChar includedChar) {
		if (!this.completedPersons.contains(includedChar.personID)) {
			this.charSetManager.handleIncludedChar(includedChar.value, includedChar.personID);
			Person person = this.persons.get(includedChar.personID);
			person.addChar(includedChar.value);
		}
	}

	private void handle(Solution solution) {
		Person person = this.persons.get(solution.personID);
		this.completedPersons.add(solution.personID);
		String message = String.format("Password of person %d (%s): %s", person.getId(), person.getName(),
										solution.solution);
		//log().info(message);
		this.collector.tell(new Collector.CollectMessage(message), this.self());
		this.persons.remove(person.getId());
		if (this.persons.size() == 0) {
			this.reader.tell(new Reader.ReadMessage(), this.self());
		}
	}

	protected void terminate() {
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
		this.sender().tell(new Worker.MasterHello(), this.self());
//		this.log().info("Registered {}", this.sender());
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}

	private static Map<Integer, Person> parseLines(List<String[]> lines) {
		Map<Integer, Person> persons = new HashMap<>();
		for (String[] line : lines) {
			Person person = Person.fromList(line);
			persons.put(person.getId(), person);
		}
		return persons;
	}

	private void sendWorkItem(ActorRef worker) {
		for (Map.Entry<Integer, Person> personEntry : this.persons.entrySet()) {
			Person person = personEntry.getValue();
			if (person.isReadyForCracking() && !person.isBeingCracked()) {
				sendCrackingRequest(worker, person);
				return;
			}
		}
		if (this.persons.size() > 0 && this.charSetManager.hasNext()) {
			try {
				sendHints(worker, collectHints());
				return;
			}
			catch (IndexOutOfBoundsException e) {
				// pass
			}
		}

		this.waitingWorkers.add(worker);
	}

	private void sendHints(ActorRef worker, Set<Hint> hints) {
		CharSetManager.CharSet charSet = this.charSetManager.next();
		Worker.PermutationsRequest request = new Worker.PermutationsRequest(
				hints,
				charSet.getSet(),
				charSet.getExcludedChar());
		worker.tell(request, this.self());
	}

	private void sendCrackingRequest(ActorRef worker, Person person) {
		Worker.CrackingRequest request = new Worker.CrackingRequest(
				person.getId(),
				person.getPasswordHash(),
				person.getPasswordLength(),
				person.getSolutionSet());
		worker.tell(request, this.self());
		person.setBeingCracked(true);
	}

	private void distributeWork() {
		Set<ActorRef> workers = new HashSet<>(this.waitingWorkers);
		for (ActorRef worker : workers) {
			this.waitingWorkers.remove(worker);
			sendWorkItem(worker);
		}
	}

	private Set<Hint> collectHints() {
		Set<Hint> result = new HashSet<>();
		for (Map.Entry<Integer, Person> person : this.persons.entrySet()) {
			if (!person.getValue().isBeingCracked()) {
				result.addAll(person.getValue().getHints());
			}
		}
		return result;
	}

}
