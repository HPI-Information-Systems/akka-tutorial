package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
        this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WelcomeMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private BloomFilter welcomeData;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackHintMessage implements Serializable {
        private static final long serialVersionUID = 8797218033667079391L;
        private String id;
        private String[] hashedHints;
        private char[] alphabet;
        private int hintLength;
        private char missingCharacter;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackPasswordMessage implements Serializable {
        private static final long serialVersionUID = 8797218033667079391L;
        private String id;
        private String hashedPassword;
        private char[] alphabet;
        private int passwordLength;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;
    private final ActorRef largeMessageProxy;
    private long registrationTime;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(WelcomeMessage.class, this::handle)
                .match(CrackHintMessage.class, this::handle)
                .match(CrackPasswordMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());

            this.registrationTime = System.currentTimeMillis();
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(WelcomeMessage message) {
        final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
        this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
    }

    // adjusted from https://stackoverflow.com/a/29910788
    private static void combinations(int n, char[] arr, List<char[]> list) {
        // Calculate the number of arrays we should create
        int numArrays = (int) Math.pow(arr.length, n);
        // Create each array
        for (int i = 0; i < numArrays; i++) {
            list.add(new char[n]);
        }
        // Fill up the arrays
        for (int j = 0; j < n; j++) {
            // This is the period with which this position changes, i.e.
            // a period of 5 means the value changes every 5th array
            int period = (int) Math.pow(arr.length, n - j - 1);
            for (int i = 0; i < numArrays; i++) {
                char[] current = list.get(i);
                // Get the correct item and set it
                int index = i / period % arr.length;
                current[j] = arr[index];
            }
        }
    }

    private void handle(CrackPasswordMessage message) {
        List<char[]> combinations = new ArrayList<>();
        combinations(message.passwordLength, message.alphabet, combinations);
        for (char[] combination : combinations) {
            if (hash(combination).equals(message.hashedPassword)) {
                this.sender().tell(new Master.CrackPasswordResultMessage(message.id, combination), this.self());
                break;
            }
        }
    }

    private static final ConcurrentHashMap<Character, List<char[]>> permutationsMap = new ConcurrentHashMap<>();

    private static void initializePermutationMap(char[] alphabet, int hintLength, char missingCharacter) {
        char[] alphabetWithoutOne = new char[alphabet.length - 1];
        for (int i = 0; i < alphabet.length; i++) {
            if (alphabet[i] == missingCharacter) {
                System.arraycopy(alphabet, 0, alphabetWithoutOne, 0, i);
                System.arraycopy(alphabet, i + 1, alphabetWithoutOne, i, alphabetWithoutOne.length - i);
            }
        }

        List<char[]> permutations = new ArrayList<>();
        heapPermutation(alphabetWithoutOne, hintLength, permutations);

        permutationsMap.put(missingCharacter, permutations);
    }

    private static List<char[]> getPermutations(char[] alphabet, int hintLength, char missingCharacter) {
        if (!permutationsMap.containsKey(missingCharacter)) {
            initializePermutationMap(alphabet, hintLength, missingCharacter);
        }
        return permutationsMap.get(missingCharacter);
    }

    private void handle(CrackHintMessage message) {
        for (char[] permutation : getPermutations(message.alphabet, message.hintLength, message.missingCharacter)) {
            String hash = hash(permutation);
            for (int i = 0; i < message.hashedHints.length; i++) {
                if (hash.equals(message.hashedHints[i])) {
                    this.sender().tell(new Master.CrackHintResultMessage(message.id, message.missingCharacter, message.hashedHints[i]), this.self());
                    return;
                }
            }
        }
        this.sender().tell(new Master.CrackHintNoResultMessage(message.id, message.missingCharacter), this.self());
    }

    MessageDigest digest;

    private String hash(char[] characters) {
        try {
            if (digest == null) {
                digest = MessageDigest.getInstance("SHA-256");
            }
            byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private static void heapPermutation(char[] a, int size, List<char[]> l) {
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