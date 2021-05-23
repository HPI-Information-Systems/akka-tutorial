package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

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
import de.hpi.ddm.actors.Master.HashSolutionMessage;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Stupid Worker, can only search for hashes of an alphabet
 */
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
	public static class InitHintCrackingConfigurationMessage implements Serializable {
		private static final long serialVersionUID = 1243040942711109598L;
		private char[] alphabet;
		private int passwordIndex;
		private int permutationCount;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class InitPwdCrackingConfigurationMessage implements Serializable {
		private static final long serialVersionUID = 1243040942788109598L;
		private char[] alphabet;
		private int[] startIndices;
		private int steps;
		private String passwordHash;
		private int passwordIndex;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HintHashesMessage implements Serializable {
		private static final long serialVersionUID = 1343040942711109598L;
		private List<String> hashes;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CrackNextNHintPermutationsMessage implements Serializable {
		private static final long serialVersionUID = 1543040942711109598L;
		private int hashCount;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CrackNextNPasswordPermutationsMessage implements Serializable {
		private static final long serialVersionUID = 1543040943451109598L;
		private int count;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadyForMoreMessage implements Serializable {
		private static final long serialVersionUID = 1843040942711109598L;
		private int passwordIndex;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class FinishedWorkingOnPasswordCrackingBatchMessage implements Serializable {
		private static final long serialVersionUID = 1843040942711102898L;
		private int passwordIndex;
	}

	@Data
	@NoArgsConstructor
	public static class FinishedPermutationsMessage implements Serializable {
		private static final long serialVersionUID = 1883040942711109598L;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;

	private char[] alphabet;
	private List<String> permutations = new ArrayList<String>();
	private HashSet<String> hashes = new HashSet<String>();
	private int permutationIndex = 0;
	private int passwordIndex = -1;
	private String passwordHash = "";
	private int[] currentPasswordIndices = new int[0];
	private int currentlyTriedPasswordCombinations = 0;
	private int maxPasswordCombinations = 0;

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
		return receiveBuilder().match(CurrentClusterState.class, this::handle).match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle).match(WelcomeMessage.class, this::handle)
				.match(InitHintCrackingConfigurationMessage.class, this::handle).match(HintHashesMessage.class, this::handle)
				.match(InitPwdCrackingConfigurationMessage.class, this::handle)
				.match(CrackNextNHintPermutationsMessage.class, this::handle)
				.match(CrackNextNPasswordPermutationsMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString())).build();
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

			this.getContext().actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
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
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in "
				+ transmissionTime + " ms.");
	}

	private void handle(InitHintCrackingConfigurationMessage message) {
		this.permutations.clear();
		this.hashes.clear();
		Worker.heapPermutation(message.alphabet, message.alphabet.length, message.permutationCount, this.permutations);
		this.passwordIndex = message.passwordIndex;
		this.permutationIndex = 0;
	}

	private void handle(InitPwdCrackingConfigurationMessage message){
		this.alphabet = message.alphabet;
		this.passwordHash = message.passwordHash;
		this.currentPasswordIndices = message.startIndices;
		this.passwordIndex = message.passwordIndex;
		this.currentlyTriedPasswordCombinations = 0;
		this.maxPasswordCombinations = message.steps;
	}

	private void handle(HintHashesMessage message) {
		this.hashes.addAll(message.hashes);
	}

	private void handle(CrackNextNPasswordPermutationsMessage message) {
		char[] currentPwd = new char[this.currentPasswordIndices.length];
		for(int steps = 0; steps < message.count && this.currentlyTriedPasswordCombinations < this.maxPasswordCombinations; ++this.currentlyTriedPasswordCombinations, ++steps){
			for(int i = 0; i < this.currentPasswordIndices.length; ++i){
				currentPwd[i] = this.alphabet[this.currentPasswordIndices[i]];
			}
			String currentPwdAsString = new String(currentPwd);
			String pwdHash = Worker.hash(currentPwdAsString);
			if(pwdHash.equals(this.passwordHash)){
				this.currentlyTriedPasswordCombinations = this.maxPasswordCombinations;
				getSender().tell(new Master.PasswordSolutionMessage(pwdHash, currentPwdAsString, this.passwordIndex), getSelf());
			}
			Worker.shiftPwdPermutation(this.currentPasswordIndices, this.alphabet.length);
		}
		if(this.currentlyTriedPasswordCombinations < this.maxPasswordCombinations){
			this.log().info("Finished working on Password Permutation Batch");
			getSender().tell(new FinishedWorkingOnPasswordCrackingBatchMessage(this.passwordIndex), getSelf());
		} else {
			this.log().info("Finished working on Password Permutation Package");
			getSender().tell(new FinishedPermutationsMessage(), getSelf());
		}
	}

	private void handle(CrackNextNHintPermutationsMessage message) {
		int end = Math.min(this.permutationIndex + message.hashCount, this.permutations.size());
		List<String> permutationSubset = this.permutations.subList(this.permutationIndex, end);
		this.permutationIndex = end;
		for (String permutationMember : permutationSubset) {
			String hash = Worker.hash(permutationMember);
			if (this.hashes.contains(hash)) {
				this.log().info("Found match");
				getSender().tell(new HashSolutionMessage(hash, permutationMember, this.passwordIndex), getSelf());
			}
		}
		if (end == this.permutations.size()) {
			this.log().info("Finished working on Permutations");
			getSender().tell(new FinishedPermutationsMessage(), getSelf());
		} else {
			this.log().info("Finished working on Permutation Batch");
			getSender().tell(new ReadyForMoreMessage(this.passwordIndex), getSelf());
		}
	}

	public static String hash(String characters) {
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

	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	public static void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size && l.size() < n; i++) {
			heapPermutation(a, size - 1, n, l);

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

	public static boolean shiftPwdPermutation(int[] alphabetIndices, int alphabetLength){
		int currentIndex = alphabetIndices.length - 1;
		boolean hasOverflow = true;
		while(hasOverflow && currentIndex >= 0){
			++alphabetIndices[currentIndex];
			hasOverflow = alphabetIndices[currentIndex] >= alphabetLength;
			if(hasOverflow){
				alphabetIndices[currentIndex] = 0;
			}
			--currentIndex;
		}
		return !hasOverflow;
	}

	public static boolean addPwdPermutation(int[] firstAlphabetIndices, int[] secondAlphabetIndices, int alphabetLength){
		/* would be nice but we do not want to write catch blocks
		if(firstAlphabetIndices.length != secondAlphabetIndices.length){
			throw new IllegalAccessException("The two provided arguments do not have the same length! First length " + firstAlphabetIndices.length + "; second length " + secondAlphabetIndices.length);
		} */
		int overflow = 0;
		for(int i = firstAlphabetIndices.length - 1; i >= 0; --i){
			int sum = firstAlphabetIndices[i] + secondAlphabetIndices[i] + overflow;
			if(sum > alphabetLength - 1){
				int remainder = sum % alphabetLength;
				overflow = (int) Math.floor((float)sum / (float) alphabetLength);
				firstAlphabetIndices[i] = remainder;
			} else {
				firstAlphabetIndices[i] = sum;
				overflow=0;
			}
		}
		boolean didNotOverflow = overflow == 0;
		return didNotOverflow;
	}

	public static boolean numberToPermutation(int number, int alphabetLength, int passwordLength, int[] alphabetIndices){
		for(int i = 0; i < passwordLength; ++i){
			alphabetIndices[i] = 0;
		}
		int index = passwordLength - 1;
		while(index >= 0 && number > 0) {
			int remainder = number % alphabetLength;
			alphabetIndices[index] = remainder;
			number = (int) Math.floor((float) number / (float) alphabetLength);
			--index;
		}
		boolean didNotOverflow = number <= 0;
		return didNotOverflow;
	}
}