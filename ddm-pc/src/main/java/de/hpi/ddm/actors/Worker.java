package de.hpi.ddm.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import de.hpi.ddm.structures.ChunkManager;
import de.hpi.ddm.structures.Hint;
import de.hpi.ddm.structures.PermutationChunk;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

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
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class MasterHello implements Serializable {
		private static final long serialVersionUID = 6508051783772496189L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PermutationsRequest implements Serializable {
		private static final long serialVersionUID = 2767978393215113993L;
		private Set<Character> charSet;
		private Character missingChar;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintSolvingRequest implements Serializable {
		private static final long serialVersionUID = 7471763615108065806L;
		private PermutationChunk chunk;
		private Set<Hint> hints;
		private int batchNumber;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackingRequest implements Serializable {
		private static final long serialVersionUID = -7373650189022842961L;
		private Integer personID;
		private String hash;
		private int length;
		private Set<Character> charSet;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
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

	@Getter @AllArgsConstructor @NoArgsConstructor
	private static class Finished extends Error {
		private static final long serialVersionUID = -6952413878046495269L;
		private String solution;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MasterHello.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(PermutationsRequest.class, this::handle)
				.match(HintSolvingRequest.class, this::handle)
				.match(CrackingRequest.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(MasterHello masterHello) {
		this.master = this.sender();
		requestWork();
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
		}
	}

	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(PermutationsRequest request) {
		char[] chars = convertCharSet(request.charSet);
		ChunkManager chunkManager = new ChunkManager(this.sender(), this.self(),
														request.missingChar, new LinkedList<>());
		heapPermutation(chars, chars.length, chars.length, chunkManager);
		chunkManager.flush();
		requestWork();
	}

	private void handle(HintSolvingRequest request) {
		hashAndCompare(request.chunk, request.hints, request.batchNumber);
		this.sender().tell(new Master.CompletedChunk(request.chunk.getMissingChar(), request.batchNumber), this.self());
		requestWork();
	}

	private void handle(CrackingRequest request) {
		try {
			crack(request.hash, request.charSet, request.length, "");
		} catch (Finished f) {
			String solution = f.getSolution();
			this.sender().tell(new Master.Solution(request.personID, solution), this.self());
		}
		requestWork();
	}

	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	private void heapPermutation(char[] a, int size, int n, ChunkManager chunkManager) {
		if (size == 1) {
			chunkManager.add(new String(a));
			return;
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, chunkManager);

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

	private void hashAndCompare(PermutationChunk chunk, Set<Hint> hints, int batchNumber) {
		for (String permutation : chunk.getPermutations()) {
			Integer personID = null;
			for (Hint hint : hints) {
				if (hasHash(permutation, hint.getValue())) {
					Master.ExcludedChar message = new Master.ExcludedChar(hint.getPersonID(), chunk.getMissingChar(),
							hint.getValue(), batchNumber);
					//log().info("Char {} is NOT in password of person {}", chunk.getMissingChar(), hint.getPersonID());
					this.master.tell(message, this.self());
					personID = hint.getPersonID();
				}
			}
			pruneHints(personID, hints);
		}
	}


	private void crack(String hash, Set<Character> charSet, int pendingChars, String prefix) {
		if (pendingChars == 0) {
			if (hasHash(prefix, hash)) {
				throw new Finished(prefix);
			}
			return;
		}

		for (Object c : charSet.toArray()) {
			crack(hash, charSet, pendingChars - 1, prefix + c);
		}
	}

	private char[] convertCharSet(Set<Character> charSet) {
		char[] chars = new char[charSet.size()];
		List<Character> list = Arrays.asList(charSet.toArray(new Character[0]));
		Collections.shuffle(list);
		for (int i = 0; i < list.size(); i++) {
			chars[i] = list.get(i);
		}
		return chars;
	}

	private boolean hasHash(String string, String hash) {
		return hash.equals(hash(string));
	}

	private void requestWork() {
		this.master.tell(new Master.WorkRequest(), this.self());
	}

	private void pruneHints(Integer personID, Set<Hint> hints) {
		if (personID == null) {
			return;
		}

		Set<Hint> toExclude = new HashSet<>();
		for (Hint hint : hints) {
			if (hint.getPersonID().equals(personID)) {
				toExclude.add(hint);
			}
		}

		hints.removeAll(toExclude);
	}

}
