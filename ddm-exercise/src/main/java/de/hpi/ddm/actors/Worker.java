package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;

import akka.actor.AbstractActor.Receive;
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
import de.hpi.ddm.actors.Worker.CrackNMessage;
import de.hpi.ddm.actors.Worker.FinishedPermutationsMessage;
import de.hpi.ddm.actors.Worker.HintHashesMessage;
import de.hpi.ddm.actors.Worker.InitConfigurationMessage;
import de.hpi.ddm.actors.Worker.ReadyForMoreMessage;
import de.hpi.ddm.actors.Worker.WelcomeMessage;
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
	public static class InitConfigurationMessage implements Serializable {
		private static final long serialVersionUID = 1243040942711109598L;
		private char[] alphabet;
		private int permutationSubSize;
		private int permutationStartSize;
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
	public static class CrackNMessage implements Serializable {
		private static final long serialVersionUID = 1543040942711109598L;
		private int hashCount;
	}

	@Data
	@NoArgsConstructor
	public static class ReadyForMoreMessage implements Serializable {
		private static final long serialVersionUID = 1843040942711109598L;
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
	private List<String> permutations;
	private HashSet hashes;
	private int permutationIndex = 0;

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
				.match(InitConfigurationMessage.class, this::handle).match(HintHashesMessage.class, this::handle)
				.match(CrackNMessage.class, this::handle)
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

	private void handle(InitConfigurationMessage message) {
		this.heapPermutation(message.alphabet, message.alphabet.length, message.alphabet.length, this.permutations);
		this.permutations = this.permutations.subList(message.permutationStartSize,
				message.permutationStartSize + message.permutationSubSize);
	}

	private void handle(HintHashesMessage message) {
		this.hashes.addAll(message.hashes);
	}

	private void handle(CrackNMessage message) {
		int end = Math.max(this.permutationIndex + message.hashCount, this.permutations.size());
		List<String> permutationSubset = this.permutations.subList(this.permutationIndex, end);
		this.permutationIndex = end;
		for (String permutationMember : permutationSubset) {
			String hash = Worker.hash(permutationMember);
			if (this.hashes.contains(hash)) {
				getSender().tell(new HashSolutionMessage(hash, permutationMember), getSelf());
			}
		}
		if (end == this.permutations.size()) {
			getSender().tell(new FinishedPermutationsMessage(), getSelf());
		} else {
			getSender().tell(new ReadyForMoreMessage(), getSelf());
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
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
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
}