package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.*;
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
    public static class CrackHintsResultMessage implements Serializable {
        private static final long serialVersionUID = 7033566951826394411L;
        private String id;
        private List<Character> missingCharacters;
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

    private CopyOnWriteArrayList<WorkerTask> pendingTasks = new CopyOnWriteArrayList<>();
    private ConcurrentMap<String, String[]> lines = new ConcurrentHashMap<>();
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
                .match(CrackHintsResultMessage.class, this::handle)
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

        Map<Character, List<char[]>> permutationsMap = new HashMap<>();

        String[] firstLine = message.getLines().get(0);
        char[] alphabet = firstLine[2].toCharArray();
        int passwordLength = Integer.parseInt(firstLine[3]);
        for (int i = 0; i < alphabet.length; i++) {
            char missingCharacter = alphabet[i];
            char[] alphabetWithoutOne = new char[alphabet.length - 1];
            System.arraycopy(alphabet, 0, alphabetWithoutOne, 0, i);
            System.arraycopy(alphabet, i + 1, alphabetWithoutOne, i, alphabetWithoutOne.length - i);

            List<char[]> permutations = new ArrayList<>();
            long s = System.currentTimeMillis();
            heapPermutation(alphabetWithoutOne, passwordLength, permutations);
            System.out.println(System.currentTimeMillis() - s);

            permutationsMap.put(missingCharacter, permutations);
        }

        this.log().info("processing input lines");
        // TODO: Process the lines with the help of the worker actors
        for (String[] line : message.getLines()) {
            lines.put(line[0], line);

            String[] hintHashes = new String[line.length - 5];
            System.arraycopy(line, 5, hintHashes, 0, hintHashes.length);

            pendingTasks.add(worker -> {
                this.log().info("start hint cracking for " + line[0]);
                this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.CrackHintsMessage(line[0], hintHashes, hintHashes.length / 2, permutationsMap), worker), this.self());
                //worker.tell(new Worker.CrackHintsMessage(line[0], hintHashes, hintHashes.length / 2, permutationsMap), this.self());
            });
        }

        doNextTask();

        // TODO: Fetch further lines from the Reader
        this.reader.tell(new Reader.ReadMessage(), this.self());

    }

    protected void handle(CrackHintsResultMessage message) {
        this.log().info("received hint crack result for " + message.id);

        availableWorkers.add(this.sender());
        pendingTasks.add(worker -> {
            List<Character> alphabetList = new ArrayList<>(Arrays.asList(ArrayUtils.toObject(lines.get(message.id)[2].toCharArray())));
            alphabetList.removeAll(message.missingCharacters);
            char[] remainingAlphabetChars = ArrayUtils.toPrimitive(alphabetList.toArray(new Character[0]));

            this.log().info("starting to crack password of " + message.id + " with remaining characters " + new String(remainingAlphabetChars));
            worker.tell(new Worker.CrackPasswordMessage(message.id, lines.get(message.id)[4], remainingAlphabetChars, Integer.parseInt(lines.get(message.id)[3])), this.self());
        });
        doNextTask();
    }

    protected void handle(CrackPasswordResultMessage message) {
        this.log().info("received password crack result for " + message.id + "(" + new String(message.password) + ")");
        availableWorkers.add(this.sender());
        doNextTask();

        this.collector.tell(new Collector.CollectMessage("user " + message.id + " has password " + new String(message.password)), this.self());
    }

    protected void doNextTask() {
        if (readAll && pendingTasks.isEmpty() && availableWorkers.size() == workerCount.get()) {
            this.log().info("no tasks and occupied workers remaining, exiting...");
            this.terminate();
            return;
        }

        while (!availableWorkers.isEmpty() && !pendingTasks.isEmpty()) {
            this.log().info(availableWorkers.size() + " available workers and " + pendingTasks.size() + " pending tasks");
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

        //this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());


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

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private void heapPermutation(char[] a, int size, List<char[]> l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            l.add(a.clone());

        for (int i = 0; i < size; i++) {
            heapPermutation(a, size - 1, l);

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
    }
}
