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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";
    private final Cluster cluster;
    private final ActorRef largeMessageProxy;

    ////////////////////
    // Actor Messages //
    ////////////////////
    private Member masterSystem;
    private long registrationTime;

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
        this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
    }

    public static Props props() {
        return Props.create(Worker.class);
    }

    /////////////////
    // Actor State //
    /////////////////

    static List<String> generateCombinations(char[] set, int k) {
        List<String> result = new ArrayList<>();
        int n = set.length;
        generateCombinationsRec(set, "", n, k, result);
        return result;
    }

    static void generateCombinationsRec(char[] set,
                                        String prefix,
                                        int n, int k, List<String> result) {
        if (k == 0) {
            result.add(prefix);
            return;
        }
        for (int i = 0; i < n; ++i) {
            String newPrefix = prefix + set[i];
            generateCombinationsRec(set, newPrefix, n, k - 1, result);
        }
    }

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(WelcomeMessage.class, this::handle)
                .match(BuildPermutationsMessage.class, this::handle)
                .match(CrackPasswordMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void tellMaster(Object message) {
        this.getContext()
                .actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
                .tell(message, this.getSelf());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    private void handle(CrackPasswordMessage message) {
        System.out.println("CrackPasswordMessage message from master " + message);
		final String[] record = message.getRecord();
		final String passwordChars = record[2];
		final int passwordLength = Integer.parseInt(record[3]);
		final String passwordHash = record[4];
        List<String> hints = Arrays.asList(record).subList(5, record.length);

        System.out.println("CrackPasswordMessage calculate not included chars");
        List<Character> notIncludedChars = new ArrayList<>();
        hints.stream().forEach(h -> {
            for (char c : passwordChars.toCharArray()) {
                if (!h.contains(String.valueOf(c))) {
                    notIncludedChars.add(c);
                }
            }
        });

        System.out.println("CrackPasswordMessage calculate included chars");
		String possiblePasswordChars = passwordChars;
		for(char c : notIncludedChars) {
			possiblePasswordChars = possiblePasswordChars.replace(String.valueOf(c), "");
		}

        System.out.println("CrackPasswordMessage possiblePasswordChars " + possiblePasswordChars);
        List<String> possiblePasswords = generateCombinations(possiblePasswordChars.toCharArray(), passwordLength);
        System.out.println("CrackPasswordMessage possiblePasswords " + possiblePasswords.size());

        System.out.println("CrackPasswordMessage possiblePasswords calculate hashes");
		Map<String, String> possiblePasswordsHashed = new HashMap<>();
		possiblePasswords.stream().forEach(p -> possiblePasswordsHashed.put(hash(p), p));
		String password = possiblePasswordsHashed.get(passwordHash);
        System.out.println("CrackPasswordMessage password " + password);

		List<String> solutionRecord = new ArrayList<>();
		for(int i = 0; i <= 3; i++) {
			solutionRecord.add(record[i]);
		}
		solutionRecord.add(password);
		solutionRecord.addAll(hints);

        System.out.println("CrackPasswordMessage finished " + solutionRecord);
		tellMaster(new Master.ReceiveResolvedRecordMessage(String.join(";", solutionRecord)));
    }

    private void handle(BuildPermutationsMessage message) {
        System.out.println("BuildPermutationsMessage from master " + message.getPasswordCharsSubset());
        List<String> permutations = new ArrayList<>();
        this.heapPermutation(message.getPasswordCharsSubset().toCharArray(), message.getPasswordCharsSubset().toCharArray().length, permutations);

        List<ResolvedHint> resolvedHints = new ArrayList<>();
        List<String[]> records = message.getRecords();

        Set<String> hashes = new HashSet<>();
        for(int row = 0; row < records.size(); row++) {
            String[] record = records.get(row);
            for(int col = 5; col < record.length; col++) {
                String cell = record[col];
                hashes.add(cell);
            }
        }

        System.out.println("BuildPermutationsMessage before resolving hashes");
        permutations.forEach(p -> {
            String hash = hash(p);
            if(hashes.contains(hash)){
                for(int row = 0; row < records.size(); row++) {
                    String[] record = records.get(row);
                    for(int col = 5; col < record.length; col++) {
                        String cell = record[col];
                        if(hash.equals(cell)) {
                            resolvedHints.add(new ResolvedHint(row, col, p));
                        }
                    }
                }
            }
        });
        System.out.println("BuildPermutationsMessage after resolving hashes");

        tellMaster(new Master.ReceivePermutationHashMessage(resolvedHints));
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

    private String hash(String characters) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes(StandardCharsets.UTF_8));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private void heapPermutation(char[] a, int size, List<String> l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            l.add(new String(a));

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    protected class ResolvedHint implements Serializable {
        private static final long serialVersionUID = 1243040942748609598L;
        int row;
        int column;
        String resolvedValue;
    }

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
    public static class BuildPermutationsMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609599L;
        private String passwordCharsSubset;
        private List<String[]> records;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BuildPermutationHashMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609599L;
        private List<String> permutations;
        private List<String[]> records;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrackPasswordMessage implements Serializable {
        private static final long serialVersionUID = 1343040942748609599L;
        private String[] record;
    }
}