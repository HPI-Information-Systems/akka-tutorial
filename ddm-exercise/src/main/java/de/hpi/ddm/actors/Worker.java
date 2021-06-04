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
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public static class CrackHintsMessage implements Serializable {
        private static final long serialVersionUID = 8797218033667079391L;
        private String id;
        private String[] hashedHints;
        private char[] alphabet;
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

    private List<Character> currentCrackedHints;
    private String[] currentLine;
    private ActorRef master;

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
                .match(CrackHintsMessage.class, this::handle)
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

    private void handle(CrackPasswordMessage message) {
        heapPermutation(message.alphabet, message.passwordLength, permutation -> {
            if(hash(permutation).equals(message.hashedPassword)){
                this.sender().tell(new Master.CrackPasswordResultMessage(message.id,permutation), this.self());
                return true;
            }
            return false;
        });
    }

    private void handle(CrackHintsMessage message) {
        List<String> remainingHashes = new ArrayList<>(Arrays.asList(message.hashedHints));
        for (int i = 0; i < message.alphabet.length; i++) {
            char missingCharacter = message.alphabet[i];
            char[] alphabetWithoutOne = new char[message.alphabet.length - 1];
            System.arraycopy(message.alphabet, 0, alphabetWithoutOne, 0, i);
            System.arraycopy(message.alphabet, i + 1, alphabetWithoutOne, i, alphabetWithoutOne.length - i);

            if (heapPermutation(alphabetWithoutOne, alphabetWithoutOne.length, permutation -> {
                if(remainingHashes.isEmpty()){
                    return true;
                }
                for(String hash: remainingHashes) {
                    if (hash(permutation).equals(hash)) {
                        remainingHashes.remove(hash);
                        this.sender().tell(new Master.CrackHintResultMessage(message.id, missingCharacter), this.self());
                    }
                }
                return false;
            })) return;
        }
    }

    private String hash(String characters) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
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

    interface PermutationCallback {
        boolean check(String s);
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private boolean heapPermutation(char[] a, int size, PermutationCallback l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            if (l.check(new String(a))) return true;

        for (int i = 0; i < size; i++) {
            if (heapPermutation(a, size - 1, l)) return true;

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

        return false;
    }
}