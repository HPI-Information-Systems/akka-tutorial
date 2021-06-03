package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.event.LoggingAdapter;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.structures.PasswordEntry;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}

	// Well... Chewbakka cannot keep the hands off it...
	@Data @NoArgsConstructor
	public static class ThanksForCrackMessage implements Serializable {
		private static final long serialVersionUID = 4576144675273774875L;
	}

	@Data @NoArgsConstructor
	public static class StopCrackMessage implements Serializable {
	}

	/**
	 * A message with the password entry to crack a password for and the number of hints to crack before
	 * cracking the password.
	 */
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = 7196274048688399161L;
		// If null, currently no work available.
		private PasswordEntry passwordEntry;
		private int numHintsToCrack;
		private boolean randomized;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;

	private boolean shouldStopCracking = false;

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
			.match(WorkMessage.class, this::handle)
			.match(ThanksForCrackMessage.class, this::handle)
			.match(StopCrackMessage.class, this::handle)
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
		if (this.masterSystem.equals(message.member())) {
			this.stopCracking();
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}

	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().warning("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");

		// This worker has nothing to do for now, send request.
		this.sender().tell(new Master.GetNextWorkItemMessage(), this.self());
	}

	private boolean crack(WorkMessage workMessage, ActorRef sender, LoggingAdapter logger, ActorRef self) {
		PasswordEntry passwordEntry = workMessage.getPasswordEntry();
		logger.warning("Cracking password entry with id = {} and name = {}...", passwordEntry.getId(), passwordEntry.getName());

		// Crack hints, if any.
		int crackedHintsCounter = 0;
		List<String> hintHashes = new ArrayList<>(passwordEntry.getHintHashes());
		List<Character> remainingUniquePasswordCharsList = passwordEntry.getPasswordChars().chars().mapToObj(c -> (char) c).collect(Collectors.toList());
		Iterator<Character> it = remainingUniquePasswordCharsList.iterator();  // the chars from this string will be excluded as we crack hints

		while (it.hasNext() && crackedHintsCounter < workMessage.getNumHintsToCrack()) {
			char nextChar = it.next();

			// Exclude a char, generate permutations of the remaining ones, and compare the hashes of the permutations with those
			// of the remaining uncracked hints.
			logger.debug("Excluding {}...", nextChar);
			String charsForPerms = passwordEntry.getPasswordChars().replaceAll(String.valueOf(nextChar), "");
			logger.debug("charsForPerms = " + charsForPerms);
			String permHash = permuteAndCompare(charsForPerms.toCharArray(), passwordEntry.getPasswordChars().length() - 1, hintHashes);
			if (permHash == null) {
				continue;
			}
			crackedHintsCounter++;
			it.remove();
			hintHashes.remove(permHash);
			logger.debug("Cracked hint, remaining unique password chars = " + remainingUniquePasswordCharsList);
		}

		String remainingUniquePasswordChars = remainingUniquePasswordCharsList.stream().map(String::valueOf).collect(Collectors.joining(""));
		logger.debug("Cracked {} hints, remaining unique password chars = {}", crackedHintsCounter, remainingUniquePasswordChars);
		logger.debug("Cracking password...");

		// Derive all combinations of possible unique password chars from the remaining unique password chars.
		// E.g., if the actual number of unique password chars = 2, then for the `remainingUniquePasswordChars` = "ABCD",
		// we will generate "AB", "AC", "AD", "BC", "BD", "CD". I.e., if we did not crack some hints (in this example, 2)
		// to precisely determine what unique characters the password consists of (or if there were not enough hints to
		// do so), knowing there are only 2 unique characters in the password, we can generate all unique 2-char
		// combinations from the characters we know might be the actual unique password characters, and then,
		// for each such combination, generate all possible passwords of length `passwordLength` as sequences
		// of these 2 characters.
		// Assumption: `remainingUniquePasswordChars` is sorted (it is guaranteed to be sorted if the chars in
		// `passwordEntry.passwordChars` string are sorted.

		// The actual number of unique password characters (e.g., 2).
		int numUniquePasswordChars = passwordEntry.getPasswordChars().length() - passwordEntry.getHintHashes().size();

		LinkedList<String> uniquePasswordCharCombs = new LinkedList<>();
		for (char c : remainingUniquePasswordChars.toCharArray()) {
			uniquePasswordCharCombs.add(String.valueOf(c));
		}
		LinkedList<String> helperQueue = new LinkedList<>();

		// Until we have combinations of the required length, take each stored unique comb of chars from the queue
		// and append each unique char to it, put back in the queue, repeat.
		while (uniquePasswordCharCombs.peek().length() != numUniquePasswordChars) {
			while (!uniquePasswordCharCombs.isEmpty()) {
				String comb = uniquePasswordCharCombs.remove();
				// Since we assume `remainingUniquePasswordChars` is sorted, we only need to append chars that alphabetically
				// appear after the last char of the combination, e.g., if the comb is "ABC", the first char we would want to
				// append is "D", for "BCD" it would be "E", and so on.
				char[] arr = comb.toCharArray();
				char lastChar = arr[arr.length - 1];
				int ix = remainingUniquePasswordChars.indexOf(lastChar) + 1;
				for (int i = ix; i < remainingUniquePasswordChars.length(); i++) {
					helperQueue.add(comb + remainingUniquePasswordChars.toCharArray()[i]);
				}
			}
			// Just swap the links rather then copying values from queue to queue and clearing the helper
			// queue / creating a new helper queue.
			LinkedList<String> tmp = uniquePasswordCharCombs;
			uniquePasswordCharCombs = helperQueue;
			helperQueue = tmp;
		}
		logger.debug("Generated {} combinations", uniquePasswordCharCombs.size());

		if (workMessage.isRandomized()) {
			logger.debug("Will shuffle the combinations.");
			Collections.shuffle(uniquePasswordCharCombs);
		}

		// Iterate over the resulting unique combinations and generate possible passwords
		for (String uniquePasswordCharComb : uniquePasswordCharCombs) {
			if (this.shouldStopCracking) {
				return false;
			}
			logger.debug("Generating and checking passwords for combinations of {}.", uniquePasswordCharComb);
			// The algorithm below is similar to that used above for working out the possible combinations of unique
			// password chars, but this time we append each char to the already generated sequences.
			LinkedList<String> possiblePasswords = new LinkedList<>();
			for (char c : uniquePasswordCharComb.toCharArray()) {
				possiblePasswords.add(String.valueOf(c));
			}
			helperQueue = new LinkedList<>();

			while (possiblePasswords.peek().length() != passwordEntry.getPasswordLength()) {
				while (!possiblePasswords.isEmpty()) {
					String comb = possiblePasswords.remove();
					for (char c : uniquePasswordCharComb.toCharArray()) {
						helperQueue.add(comb + c);
					}
				}
				LinkedList<String> tmp = possiblePasswords;
				possiblePasswords = helperQueue;
				helperQueue = tmp;
			}
			logger.debug("Generated {} possible passwords.", possiblePasswords.size());

			for (String possiblePassword : possiblePasswords) {
				if (this.shouldStopCracking) {
					return false;
				}
				if (hash(possiblePassword).equals(passwordEntry.getPasswordHash())) {
					// Send the cracked password to the master.
					logger.warning("Cracked password for id = {} and name = {}, sending result to master...", passwordEntry.getId(), passwordEntry.getName());
					sender.tell(
						new Master.CrackedPasswordMessage(passwordEntry.getId(), passwordEntry.getName(), possiblePassword),
						self
					);
					return true;
				}
			}
		}
		logger.error("The password with hash {} could not be cracked.", passwordEntry.getPasswordHash());
		return false;
	}

	private void handle(WorkMessage workMessage) throws Exception {
		if (workMessage.getPasswordEntry() == null) {
			this.log().debug("Got work message without content, trying again later...");
			// Currently no work, ask master again later.
			this.getContext().system().scheduler().scheduleOnce(
				Duration.ofMillis(500),
				this.sender(),
				new Master.GetNextWorkItemMessage(),
				this.getContext().dispatcher(),
				this.self()
			);
			return;
		}

		this.shouldStopCracking = false;

		// Start cracking process (and do not block the worker).
		final WorkMessage workMessageCopy = workMessage;
		final ActorRef sender = this.sender();
		final LoggingAdapter logger = this.log();
		final ActorRef selfRef = this.self();
		this.getContext().getSystem().scheduler().scheduleOnce(
			Duration.ZERO,
			() -> this.crack(workMessageCopy, sender, logger, selfRef),
			this.getContext().dispatcher()
		);
	}

	private void handle(ThanksForCrackMessage message) {
		// We need more crack.
		this.sender().tell(new Master.GetNextWorkItemMessage(), this.self());
	}

	private void handle(StopCrackMessage message) {
		this.stopCracking();
	}

	private void stopCracking() {
		this.log().info("Stopping crack task.");
		this.shouldStopCracking = true;
		this.sender().tell(new Master.GetNextWorkItemMessage(), this.self());
	}

	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));

			StringBuilder stringBuffer = new StringBuilder();
			for (byte hashedByte : hashedBytes) {
				stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * @return the hash of the permutation that matched one of the hint hashes
	 */
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private String permuteAndCompare(char[] a, int size, List<String> hintHashes) {
		// If size is 1, compare the hash of the obtained permutation with the hint hashes
		String permHash;
		if (size == 1) {
			permHash = this.hash(new String(a));
			if (hintHashes.contains(permHash)) {
				return permHash;
			}
		}

		for (int i = 0; i < size; i++) {
			permHash = permuteAndCompare(a, size - 1, hintHashes);
			if (permHash != null) {
				return permHash;
			}

			// If size is odd, swap first and last element
			char temp;
			if (size % 2 == 1) {
				temp = a[0];
				a[0] = a[size - 1];
			}

			// If size is even, swap i-th and last element
			else {
				temp = a[i];
				a[i] = a[size - 1];
			}
			a[size - 1] = temp;
		}
		return null;
	}
}