package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackHintResultMessage implements Serializable {
        private static final long serialVersionUID = 7033566951826394411L;
        private String id;
        private char missingCharacter;
        private String hashedHint;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackHintNoResultMessage implements Serializable {
        private static final long serialVersionUID = 606002318005377669L;
        private String id;
        private char missingCharacter;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackPasswordResultMessage implements Serializable {
        private static final long serialVersionUID = 6194489711561811313L;
        private String id;
        private char[] password;
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

    interface WorkerTask {
        public void run(ActorRef worker);
    }

    private ConcurrentMap<String, String> passwords = new ConcurrentHashMap<>();
    private ConcurrentMap<String, char[]> alphabets = new ConcurrentHashMap<>();
    private ConcurrentMap<String, char[]> remainingCharacters = new ConcurrentHashMap<>();
    private ConcurrentMap<String, char[]> remainingPasswordCharacters = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Integer> passwordLengths = new ConcurrentHashMap<>();
    private ConcurrentMap<String, String[]> remainingHints = new ConcurrentHashMap<>();

    private CopyOnWriteArrayList<WorkerTask> pendingTasks = new CopyOnWriteArrayList<>();
    private AtomicInteger workerCount = new AtomicInteger(0);
    private CopyOnWriteArrayList<ActorRef> availableWorkers = new CopyOnWriteArrayList<>();
    private boolean readAll = false;

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
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(CrackHintResultMessage.class, this::handle)
                .match(CrackHintNoResultMessage.class, this::handle)
                .match(CrackPasswordResultMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        this.reader.tell(new Reader.ReadMessage(), this.self());
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

        // TODO: Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
        if (message.getLines().isEmpty()) {
            //this.terminate();
            readAll = true;
            return;
        }

        this.log().info("processing input lines");
        // TODO: Process the lines with the help of the worker actors
        for (String[] line : message.getLines()) {
            String id = line[0];
            char[] alphabet = line[2].toCharArray();
            int hintLength = Integer.parseInt(line[3]);

            String[] hintHashes = new String[line.length - 5];
            System.arraycopy(line, 5, hintHashes, 0, hintHashes.length);

            passwords.put(id, line[4]);
            alphabets.put(id, alphabet);
            remainingCharacters.put(id, alphabet);
            remainingPasswordCharacters.put(id, new char[0]);
            remainingHints.put(id, hintHashes);
            passwordLengths.put(id, hintLength);

            pendingTasks.add(worker -> {
                //this.log().info("start hint cracking for " + id);
                worker.tell(new Worker.CrackHintMessage(id, hintHashes, alphabet, hintLength, alphabet[0]), this.self());
            });
        }

        doNextTask();

        // TODO: Fetch further lines from the Reader
        this.reader.tell(new Reader.ReadMessage(), this.self());

    }

    protected void handle(CrackHintResultMessage message) {
        this.log().info("received hint crack result for " + message.id + " and character " + message.missingCharacter);

        availableWorkers.add(this.sender());

        char[] alphabet = remainingCharacters.get(message.id);
        char[] remainingAlphabetChars = new char[alphabet.length - 1];
        for (int i = 0; i < alphabet.length; i++) {
            if (alphabet[i] == message.missingCharacter) {
                System.arraycopy(alphabet, 0, remainingAlphabetChars, 0, i);
                System.arraycopy(alphabet, i + 1, remainingAlphabetChars, i, remainingAlphabetChars.length - i);
            }
        }
        remainingCharacters.put(message.id, remainingAlphabetChars);

        String[] hints = remainingHints.get(message.id);
        String[] remainingHintsArray = new String[hints.length - 1];
        for (int i = 0; i < hints.length; i++) {
            if (hints[i].equals(message.hashedHint)) {
                System.arraycopy(hints, 0, remainingHintsArray, 0, i);
                System.arraycopy(hints, i + 1, remainingHintsArray, i, remainingHintsArray.length - i);
            }
        }
        remainingHints.put(message.id, remainingHintsArray);

        if (remainingHintsArray.length <= 2) {
            pendingTasks.add(worker -> {
                char[] remainingPasswordChars = remainingPasswordCharacters.get(message.id);
                char[] passwordAlphabet = new char[remainingAlphabetChars.length + remainingPasswordChars.length];
                System.arraycopy(remainingPasswordChars, 0, passwordAlphabet, 0, remainingPasswordChars.length);
                System.arraycopy(remainingAlphabetChars, 0, passwordAlphabet, remainingPasswordChars.length, remainingAlphabetChars.length);
                this.log().info("starting to crack password of " + message.id + " with remaining characters " + new String(passwordAlphabet));
                worker.tell(new Worker.CrackPasswordMessage(message.id, passwords.get(message.id), passwordAlphabet, passwordLengths.get(message.id)), this.self());
            });
        } else {
            pendingTasks.add(worker -> {
                //this.log().info("start hint cracking for " + message.id);
                worker.tell(new Worker.CrackHintMessage(message.id, remainingHintsArray, alphabets.get(message.id), passwordLengths.get(message.id), remainingAlphabetChars[0]), this.self());
            });
        }

        doNextTask();
    }

    protected void handle(CrackHintNoResultMessage message) {
        this.log().info("received no hint crack result for " + message.id + " and character " + message.missingCharacter);

        availableWorkers.add(this.sender());

        char[] alphabet = remainingCharacters.get(message.id);
        char[] remainingAlphabetChars = new char[alphabet.length - 1];
        for (int i = 0; i < alphabet.length; i++) {
            if (alphabet[i] == message.missingCharacter) {
                System.arraycopy(alphabet, 0, remainingAlphabetChars, 0, i);
                System.arraycopy(alphabet, i + 1, remainingAlphabetChars, i, remainingAlphabetChars.length - i);
            }
        }
        remainingCharacters.put(message.id, remainingAlphabetChars);

        char[] passwordChars = remainingPasswordCharacters.get(message.id);
        char[] newPasswordChars = new char[passwordChars.length + 1];
        System.arraycopy(passwordChars, 0, newPasswordChars, 0, passwordChars.length);
        newPasswordChars[passwordChars.length] = message.missingCharacter;
        remainingPasswordCharacters.put(message.id, newPasswordChars);

        pendingTasks.add(worker -> {
            //this.log().info("start hint cracking for " + message.id);
            worker.tell(new Worker.CrackHintMessage(message.id, remainingHints.get(message.id), alphabets.get(message.id), passwordLengths.get(message.id), remainingAlphabetChars[0]), this.self());
        });

        doNextTask();
    }

    protected void handle(CrackPasswordResultMessage message) {
        this.log().info("received password crack result for " + message.id + "(" + new String(message.password) + ")");

        this.collector.tell(new Collector.CollectMessage("user " + message.id + " has password " + new String(message.password)), this.self());

        availableWorkers.add(this.sender());
        doNextTask();
    }

    protected void doNextTask() {
        if (readAll && pendingTasks.isEmpty() && availableWorkers.size() == workerCount.get()) {
            this.log().info("no tasks and no occupied workers remaining, exiting...");
            this.terminate();
            return;
        }

        while (!availableWorkers.isEmpty() && !pendingTasks.isEmpty()) {
            //this.log().info(availableWorkers.size() + " available workers and " + pendingTasks.size() + " pending tasks");
            WorkerTask t = pendingTasks.remove(0);
            ActorRef w = availableWorkers.remove(0);
            t.run(w);
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
        this.availableWorkers.add(this.sender());
        this.workerCount.incrementAndGet();
        doNextTask();
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());

        this.availableWorkers.remove(message.getActor());
        this.workerCount.getAndDecrement();
    }
}
