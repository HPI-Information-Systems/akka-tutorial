package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import de.hpi.ddm.structures.PasswordIntel;

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
		this.lines = new ArrayList<>();
		this.currentPasswordIndex = 0;
		initConfigMessageQueue = new LinkedList<Worker.InitConfigurationMessage>();
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

	@Data @NoArgsConstructor
	public static class FinishedReadingMessage implements Serializable {
		private static final long serialVersionUID = 8343040968748609598L;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HashSolutionMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
		private String hash;
		private String clearText;
		private int passwordIndex;
	}



	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;
	private final List<String[]> lines;
	private PasswordIntel[] passwordIntels;
	private int currentPasswordIndex;
	private Queue<Worker.InitConfigurationMessage> initConfigMessageQueue;
	private String[] passwords;
	private boolean finishedHintCracking = false;

	private long startTime;
	
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
				.match(FinishedReadingMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(Worker.ReadyForMoreMessage.class, this::handle)
				.match(HashSolutionMessage.class, this::handle)
				.match(Worker.FinishedPermutationsMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	public List<String[]> getLines(){
		return this.lines;
	}
	
	protected void handle(BatchMessage message) {
		// We assume that all lines fit in memory. If they do not, our system crashes in 100% of all cases.
		List<String[]> lines = message.getLines();
		if(lines.isEmpty()){
			this.self().tell(new FinishedReadingMessage(), ActorRef.noSender());
		} else {
			this.lines.addAll(lines);
			this.reader.tell(new Reader.ReadMessage(), this.self());
		}
		
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


		/*
		// TODO: Process the lines with the help of the worker actors
		for (String[] line : message.getLines())
			this.log().error("Need help processing: {}", Arrays.toString(line));
		
		// TODO: Send (partial) results to the Collector
		this.collector.tell(new Collector.CollectMessage("If I had results, this would be one."), this.self());
		
		// TODO: Fetch further lines from the Reader
		this.reader.tell(new Reader.ReadMessage(), this.self());
		*/
		
	}

	boolean generateNextTaskSet(){
		if(!this.initConfigMessageQueue.isEmpty()){
			return true;
		}
		if(this.currentPasswordIndex == this.passwordIntels.length - 1){
			return false;
		}
		this.initConfigMessageQueue.clear();
		PasswordIntel currentPassword = this.passwordIntels[this.currentPasswordIndex];
		// Generate all alphabets
		String alphabetASString = currentPassword.getAlphabet();
		char[][] alphabets = new char[alphabetASString.length()][];
		for(int i = 0; i < alphabetASString.length(); ++i){
			String subAlphabet = alphabetASString.substring(0, i) + alphabetASString.substring(i + 1);
			alphabets[i] = subAlphabet.toCharArray();
		}
		int numberOfPermutations = 1;
		for (int i = 2; i < alphabetASString.length(); i++) {
			numberOfPermutations = numberOfPermutations * i;
		}
		int packageSize = 5000;
		for(char[] alphabet : alphabets){
			for(int currentIndex = 0; currentIndex <= numberOfPermutations; currentIndex += packageSize){
				Worker.InitConfigurationMessage currentMessage = new Worker.InitConfigurationMessage(alphabet, this.currentPasswordIndex, packageSize, currentIndex);
				this.initConfigMessageQueue.add(currentMessage);
			}
		}
		this.currentPasswordIndex++;
		return true;
	}

	Worker.CrackNMessage getCrackMessage(){
		return new Worker.CrackNMessage(500);
	}

	void handle(FinishedReadingMessage message) {
		// Init intels
		this.passwordIntels = new PasswordIntel[this.lines.size()];
		this.passwords = new String[this.lines.size()];

		for (String[] csvLine: this.lines) {
			int index = Integer.parseInt(csvLine[0]) - 1;
			this.passwordIntels[index] = new PasswordIntel(csvLine);
		}
		this.log().info("Starting with " + this.workers.size() + " workers the cracking job.");
		this.generateNextTaskSet();
		for(ActorRef worker : this.workers){
			// Ensure we have generated enough tasks.
			if(this.initConfigMessageQueue.size() == 0){
				boolean stillHasTasks = this.generateNextTaskSet();
				if(!stillHasTasks){
					return;
				}
			}
			Worker.InitConfigurationMessage initMessage = this.initConfigMessageQueue.remove();
			worker.tell(initMessage, getSelf());
			worker.tell(this.getCrackMessage(), getSelf());
		}

		// Create workers
		// On registration message
			// init worker with alphabet and all hashes
			// init worker with range to search for
			// allow worker to start
		
			// 


		this.log().info("Send initial jobs.");
	}

	void handle(HashSolutionMessage message){
		String clearText = message.getClearText();
		int index = message.getPasswordIndex();
		PasswordIntel currentPwd = this.passwordIntels[index];
		currentPwd.setUncrackedHashCounter(currentPwd.getUncrackedHashCounter() - 1);
		currentPwd.addFalseChar(clearText);
		if(index == this.currentPasswordIndex && currentPwd.getUncrackedHashCounter() == 0){
			this.initConfigMessageQueue.clear();
			this.generateNextTaskSet();
		}
	}

	void handle(Worker.ReadyForMoreMessage message){
		int passwordIndex = message.getPasswordIndex();
		PasswordIntel currentPwd = this.passwordIntels[passwordIndex];
		if(currentPwd.getUncrackedHashCounter() == 0){
			boolean hasNewTask = this.generateNextTaskSet();
			if(hasNewTask) {
				Worker.InitConfigurationMessage nextInitMessage = this.initConfigMessageQueue.remove();
				getSender().tell(nextInitMessage, getSelf());
				getSender().tell(this.getCrackMessage(), getSelf());
			}
		} else {
			getSender().tell(this.getCrackMessage(), getSelf());
		}
	}

	void handle(Worker.FinishedPermutationsMessage message){
		boolean hasNewTask = this.generateNextTaskSet();
		if(hasNewTask){
			Worker.InitConfigurationMessage nextInitMessage = this.initConfigMessageQueue.remove();
			getSender().tell(nextInitMessage, getSelf());
			getSender().tell(this.getCrackMessage(), getSelf());
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
}
