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
    public static final int PACKAGE_SIZE = 20000000;
    public static final int BATCH_SIZE =   1000000;
    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
        return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
    }

    public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
        this.idleWorkers = new LinkedList<>();
        this.lines = new ArrayList<>();
        this.currentPasswordHintCrackingIndex = 0;
        this.initHintCrackingConfigMessageQueue = new LinkedList<>();
        this.initPwdCrackingConfigMessageQueue = new LinkedList<>();
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    @NoArgsConstructor
    public static class FinishedReadingMessage implements Serializable {
        private static final long serialVersionUID = 8343040968748609598L;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintHashSolutionMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
        private String hash;
        private String clearText;
        private int passwordIndex;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordSolutionMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659194597L;
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
    private final Queue<ActorRef> idleWorkers;
    private final ActorRef largeMessageProxy;
    private final BloomFilter welcomeData;
    private final List<String[]> lines;
    private PasswordIntel[] passwordIntels;
    private int currentPasswordHintCrackingIndex;
    private int currentPasswordCrackingIndex = 0;
    private final Queue<Worker.InitHintCrackingConfigurationMessage> initHintCrackingConfigMessageQueue;
    private final Queue<Worker.InitPwdCrackingConfigurationMessage> initPwdCrackingConfigMessageQueue;
    private boolean finishedDistributingHintCrackingJobs = false;
    private boolean readAllLines = false;

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
                .match(HintHashSolutionMessage.class, this::handle)
                .match(Worker.FinishedPermutationsMessage.class, this::handle)
                .match(PasswordSolutionMessage.class, this::handle)
                .match(Worker.FinishedWorkingOnPasswordCrackingBatchMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    public List<String[]> getLines() {
        return this.lines;
    }

    protected void handle(BatchMessage message) {
        // We assume that all lines fit in memory. If they do not, our system crashes in 100% of all cases.
        List<String[]> lines = message.getLines();
        if (lines.isEmpty()) {
            this.self().tell(new FinishedReadingMessage(), ActorRef.noSender());
        } else {
            this.lines.addAll(lines);
            this.reader.tell(new Reader.ReadMessage(), this.self());
        }
    }

    boolean generateNextHintCrackingTaskSet() {
        if (!this.initHintCrackingConfigMessageQueue.isEmpty()) {
            return true;
        }
        if (this.currentPasswordHintCrackingIndex == this.passwordIntels.length) {
            return false;
        }
        PasswordIntel currentPassword = this.passwordIntels[this.currentPasswordHintCrackingIndex];
        // Generate all alphabets
        String alphabetASString = currentPassword.getAlphabet();
        char[][] alphabets = new char[alphabetASString.length()][];
        for (int i = 0; i < alphabetASString.length(); ++i) {
            String subAlphabet = alphabetASString.substring(0, i) + alphabetASString.substring(i + 1);
            alphabets[i] = subAlphabet.toCharArray();
        }
        List<String> permutations = new ArrayList<String>();
        for (char[] alphabet : alphabets) {
            permutations.clear();
            Worker.heapPermutation(alphabet, alphabet.length, alphabet.length, permutations);
            int currentIndex = 0;
            for (; currentIndex <= permutations.size(); currentIndex += PACKAGE_SIZE) {
                Worker.InitHintCrackingConfigurationMessage currentMessage = new Worker.InitHintCrackingConfigurationMessage(permutations.get(currentIndex).toCharArray(), this.currentPasswordHintCrackingIndex, PACKAGE_SIZE);
                this.initHintCrackingConfigMessageQueue.add(currentMessage);
            }
        }
        this.currentPasswordHintCrackingIndex++;
        return true;
    }

    boolean solvedAllPasswords() {
        for(PasswordIntel pwd : this.passwordIntels){
            if(pwd.getPwdSolution() == null){
                return false;
            }
        }
        return true;
    }

    boolean generateNextPasswordCrackingTaskSet() {
        if (!this.initPwdCrackingConfigMessageQueue.isEmpty()) {
            return true;
        }
        boolean foundReadyAndUnsolvedPassword = false;
        for (int i = 0; i < this.passwordIntels.length && !foundReadyAndUnsolvedPassword; ++i) {
            PasswordIntel currentPassword = this.passwordIntels[i];
            boolean solvedAllHints = currentPassword.getUncrackedHashCounter() == 0;
            if (solvedAllHints && currentPassword.getPwdSolution() == null) {
                this.currentPasswordCrackingIndex = i;
                int startingPermutationAsNumber = 0;
                char[] alphabetOfPassword = currentPassword.getAlphabetOfPwd();
				boolean didNotOverflow = true;
				while(didNotOverflow) {
					int[] alphabetIndices = new int[currentPassword.getPwdLength()];
					didNotOverflow = Worker.numberToPermutation(startingPermutationAsNumber, alphabetOfPassword.length, currentPassword.getPwdLength(), alphabetIndices);
					if (!didNotOverflow) {
						break;
					}
                    foundReadyAndUnsolvedPassword = true;
					Worker.InitPwdCrackingConfigurationMessage message = new Worker.InitPwdCrackingConfigurationMessage(alphabetOfPassword,
							alphabetIndices,
							PACKAGE_SIZE,
							currentPassword.getPwdHash(), i);
					this.initPwdCrackingConfigMessageQueue.add(message);
					startingPermutationAsNumber += PACKAGE_SIZE;
				}
            }
        }
        return foundReadyAndUnsolvedPassword;
    }

    void handle(FinishedReadingMessage message) {
        // Init intels
        this.readAllLines = true;
        this.passwordIntels = new PasswordIntel[this.lines.size()];

        for (String[] csvLine : this.lines) {
            int index = Integer.parseInt(csvLine[0]) - 1;
            this.passwordIntels[index] = new PasswordIntel(csvLine);
        }

        for (ActorRef worker : this.workers) {
            // Ensure we have generated enough tasks.
            this.tellWorkerNextJob(worker);
        }
    }

    void tellNextHintCrackingPart(ActorRef worker) {
        boolean hasTasks = this.generateNextHintCrackingTaskSet();
        if (hasTasks) {
            Worker.InitHintCrackingConfigurationMessage initMessage = this.initHintCrackingConfigMessageQueue.remove();
            worker.tell(initMessage, getSelf());
            PasswordIntel pwd = this.passwordIntels[initMessage.getPasswordIndex()];
            String[] hints = pwd.getHintHashes();
            int hintHashSize = 50;
            // Sending hash hints to worker
			List<String> hintsForWorker = new ArrayList<String>(hintHashSize);
            for (String hint : hints) {
                hintsForWorker.add(hint);
                if (hintsForWorker.size() == hintHashSize) {
                    Worker.HintHashesMessage hintHashMessage = new Worker.HintHashesMessage(hintsForWorker);
                    worker.tell(hintHashMessage, getSelf());
                    hintsForWorker.clear();
                }
            }
            if (hintsForWorker.size() > 0) {
                Worker.HintHashesMessage hintHashMessage = new Worker.HintHashesMessage(hintsForWorker);
                worker.tell(hintHashMessage, getSelf());
            }
            // Telling worker to start
            worker.tell(new Worker.CrackNextHintPermutationsMessage(), getSelf());
        } else {
        	this.finishedDistributingHintCrackingJobs = true;
        	this.tellWorkerNextJob(worker);
        }
    }

    void tellNextPasswordCrackingJob(ActorRef worker){
    	boolean hasTask = this.generateNextPasswordCrackingTaskSet();
    	if(hasTask){
    		Worker.InitPwdCrackingConfigurationMessage initMessage = this.initPwdCrackingConfigMessageQueue.remove();
    		// Init the worker to crack parts of a password.
    		worker.tell(initMessage, getSelf());
    		// Tell the worker to start.
			worker.tell(new Worker.CrackNextPasswordPermutationsMessage(), getSelf());
		} else {
    	    // As all hints are cracked and there currently no password cracking task available, but that worker to idle.
            this.idleWorkers.add(worker);
		}
	}

	void tellWorkerNextJob(ActorRef worker){
    	if(!this.finishedDistributingHintCrackingJobs){
    		this.tellNextHintCrackingPart(worker);
		} else {
    	    // If no hint jobs are available anymore try to give the worker a password cracking job.
    		this.tellNextPasswordCrackingJob(worker);
		}
    	// No jobs are available anymore. Lets wait for the remaining jobs.
	}

    void handle(HintHashSolutionMessage message) {
        String clearText = message.getClearText();
        String hash = message.getHash();
        int index = message.getPasswordIndex();
        PasswordIntel currentPwd = this.passwordIntels[index];
        currentPwd.setUncrackedHashCounter(currentPwd.getUncrackedHashCounter() - 1);
        char missingChar = currentPwd.addFalseChar(clearText, hash);
        // this.log().info("Found hash for hint " + hash + ", index {}, clear text {}, {} is missing.", index, clearText, Character.toString(missingChar));
        if (index == this.currentPasswordHintCrackingIndex && currentPwd.getUncrackedHashCounter() == 0) {
            this.initHintCrackingConfigMessageQueue.clear();
            // this.log().info("Found all hints for index {}", index);
        }
        if(currentPwd.getUncrackedHashCounter() == 0){
            // As a passwords hints are cracked and this creates new pwd cracking jobs try to deliver them to the idle workers.
            while(!this.idleWorkers.isEmpty()){
                this.tellWorkerNextJob(this.idleWorkers.remove());
            }
        }
    }

    void handle(PasswordSolutionMessage message) {
        String clearText = message.getClearText();
        int index = message.getPasswordIndex();
        PasswordIntel currentPwd = this.passwordIntels[index];
        currentPwd.setPwdClearText(clearText);
        // this.log().info("Found hash for pwd {}: {}", index, clearText);
        if (index == this.currentPasswordCrackingIndex) {
            this.initPwdCrackingConfigMessageQueue.clear();
        }
        boolean foundUncracked = false;
        for(PasswordIntel pwd : this.passwordIntels){
            if(pwd.getPwdSolution() == null){
                foundUncracked = true;
            }
        }
        if(!foundUncracked){
            // this.log().info("Found all password clear texts!");
            this.sendSolutionsToCollector();
            this.terminate();
        }
    }

    void sendSolutionsToCollector(){
        for(PasswordIntel pwdIntel : this.passwordIntels){
            this.collector.tell(new Collector.CollectMessage(pwdIntel.toString()), getSelf());
        }
        // Finally print everything
    }

    void handle(Worker.ReadyForMoreMessage message) {
        int passwordIndex = message.getPasswordIndex();
        PasswordIntel currentPwd = this.passwordIntels[passwordIndex];
        if (currentPwd.getUncrackedHashCounter() == 0) {
            this.tellWorkerNextJob(getSender());
        } else {
            getSender().tell(new Worker.CrackNextHintPermutationsMessage(), getSelf());
        }
    }

    void handle(Worker.FinishedWorkingOnPasswordCrackingBatchMessage message) {
        int passwordIndex = message.getPasswordIndex();
        PasswordIntel currentPwd = this.passwordIntels[passwordIndex];
        if (currentPwd.getPwdSolution() != null) {
            // As the password is already solved, assign a new task.
            this.tellWorkerNextJob(getSender());

        } else {
            // Let the worker continue with cracking the password batch.
            getSender().tell(new Worker.CrackNextPasswordPermutationsMessage(), getSelf());
        }
    }

    void handle(Worker.FinishedPermutationsMessage message) {
        this.tellWorkerNextJob(getSender());
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
        if(this.readAllLines){
            // only pass a task to a new worker if all lines have been read.
            this.tellWorkerNextJob(this.sender());
        }
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());
    }
}
