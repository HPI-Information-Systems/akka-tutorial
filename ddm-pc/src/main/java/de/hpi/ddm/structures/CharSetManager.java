package de.hpi.ddm.structures;

import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.*;

public class CharSetManager {

    @Getter @AllArgsConstructor
    public static class CharSet {
        private final Set<Character> set;
        private final Character excludedChar;
    }

    private final List<CharSet> charSets;
    private final Map<Character, Pair<Set<Integer>, Set<Hint>>> solutionInfo;
    private final int batchSize;
    private int currentIndex;

    public static CharSetManager fromMessageLine(String[] line, int batchSize) {
        return new CharSetManager(line[2], batchSize);
    }

    private CharSetManager(String chars, int batchSize) {
        this.charSets = generateSubsets(parseChars(chars));
        this.solutionInfo = new HashMap<>();
        initializeSolutionInfo();
        this.batchSize = batchSize;
        this.currentIndex = 0;
    }

    public void handleIncludedChar(char c, Integer personID) {
        this.solutionInfo.get(c).getKey().add(personID);
    }

    public void handleExcludedChar(char c, Integer personID, String hash) {
        this.solutionInfo.get(c).getValue().add(new Hint(personID, hash));
    }

    public boolean hasNext() {
        boolean anySetNotCompleted = false;
        for(Character c : this.solutionInfo.keySet()) {
            anySetNotCompleted = anySetNotCompleted || isNotCompleted(c);
        }
        return anySetNotCompleted;
    }

    public CharSet next() throws IndexOutOfBoundsException {
        if (!hasNext()) {
            throw new IndexOutOfBoundsException("No more CharSets left");
        }
        int index = this.currentIndex;
        this.currentIndex = getNextIndex(index);

        if (isNotCompleted(this.charSets.get(currentIndex).excludedChar)) {
            return this.charSets.get(currentIndex);
        }
        return next();
    }

    public static Set<Character> parseChars(String string) {
        Set<Character> set = new HashSet<>();
        for (int i = 0; i < string.length(); i++) {
            set.add(string.charAt(i));
        }
        return set;
    }

    private static List<CharSet> generateSubsets(Set<Character> chars) {
        List<CharSet> charSets = new LinkedList<>();
        for (Character c : chars) {
            Set<Character> set = new HashSet<>(chars);
            set.remove(c);
            charSets.add(new CharSet(set, c));
        }
        return charSets;
    }

    private void initializeSolutionInfo() {
        for(CharSet charSet : this.charSets) {
            this.solutionInfo.put(charSet.excludedChar, new Pair<>(new HashSet<>(), new HashSet<>()));
        }
    }

    private boolean isNotCompleted(Character c) {
        Pair<Set<Integer>, Set<Hint>> solutionInfo =  this.solutionInfo.get(c);
        return solutionInfo.getKey().size() + solutionInfo.getValue().size() < this.batchSize;
    }

    private int getNextIndex(int current) {
        return (current + 1) % this.charSets.size();
    }

}