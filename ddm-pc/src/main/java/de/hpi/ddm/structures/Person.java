package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter @AllArgsConstructor
public class Person {

	private final Integer id;
	private final String name;
	private final int passwordLength;
	private final Set<Character> charSet;
	private final Set<Character> solutionSet = new HashSet<>();
	private final Set<Character> excludedChars = new HashSet<>();
	private final String passwordHash;
	private final Set<Hint> hints;
	private final int solutionSize;
	@Setter boolean beingCracked;

	public static Person fromList(String[] list) {
		Integer id = Integer.valueOf(list[0]);
		int length = Integer.parseInt(list[3]);
		Set<Hint> hints = getHintHashes(id, list);
		Set<Character> charSet = CharSetManager.parseChars(list[2]);
		int solutionSize = charSet.size() - hints.size();
		return new Person(id, list[1], length, charSet, list[4], hints, solutionSize,false);
	}

	public void addChar(char c) {
		this.solutionSet.add(c);
	}

	public void dropChar(char c) {
		this.excludedChars.add(c);
	}

	public boolean isReadyForCracking() {
		if (this.solutionSet.size() == this.solutionSize) {
			return true;
		}
		if (this.charSet.size() - this.excludedChars.size() <= Math.max(this.solutionSize, 4)) {
			this.solutionSet.addAll(this.charSet);
			this.solutionSet.removeAll(this.excludedChars);
			return true;
		}
		return false;
	}

	private static Set<Hint> getHintHashes(Integer personID, String[] list) {
		Set<Hint> result = new HashSet<>();
		for (String string : Arrays.asList(list).subList(5, list.length)) {
			result.add(new Hint(personID, string));
		}
		return result;
	}



}
