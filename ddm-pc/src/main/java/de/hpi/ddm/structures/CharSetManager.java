package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.math3.util.CombinatoricsUtils;

import java.util.*;

public class CharSetManager {

    @Getter @AllArgsConstructor
    public static class CharSet {
        private final Set<Character> set;
        private final Character excludedChar;
    }

    private static class ChunkQueue {
        private Queue<PermutationChunk> queue = new LinkedList<>();
        @Getter @Setter private boolean requested = false;
        private int deliveredChunks = 0;
        private Set<Integer> excludedPersons = new HashSet<>();
        private final int batchSize;
        private final int chunkCount;

        ChunkQueue(int batchSize, int chunkCount) {
            this.batchSize = batchSize;
            this.chunkCount = chunkCount;
        }

        void push(PermutationChunk chunk) {
            if(!isFinished()) {
                this.queue.add(chunk);
            }
        }

        PermutationChunk pop() {
            return this.queue.remove();
        }

        void ack() {
            this.deliveredChunks++;
            checkFinish();
        }

        void exclude(Integer personID) {
            this.excludedPersons.add(personID);
            checkFinish();
        }

        boolean isFinished() {
            return this.deliveredChunks == chunkCount || this.excludedPersons.size() == this.batchSize;
        }

        boolean hasNext() {
            return !this.queue.isEmpty();
        }

        Set<Integer> includingPersons(Set<Integer> allIDs) {
            Set<Integer> result = new HashSet<>(allIDs);
            result.removeAll(this.excludedPersons);
            return result;
        }

        public boolean hadHashFor(Integer personID) {
            return this.excludedPersons.contains(personID);
        }

        private void checkFinish() {
            if(isFinished()) {
                this.queue = new LinkedList<>();
            }
        }
    }

    private final Queue<CharSet> charSets;
    private final Set<Integer> personIDs;
    private final Map<Character, ChunkQueue> queues;
    @Getter private boolean busy = false;

    public static CharSetManager fromMessageLine(String[] line, Set<Integer> personIDs) {
        return new CharSetManager(line[2], personIDs);
    }

    private CharSetManager(String chars, Set<Integer> personIDs) {
        this.charSets = generateSubsets(parseChars(chars));
        this.personIDs = personIDs;
        this.queues = initializeQueues(personIDs.size(),  getChunkCount(chars.length() - 1));
    }

    public void handleExcludedChar(char c, Integer personID) {
        this.queues.get(c).exclude(personID);
        this.busy = !this.queues.get(c).isFinished();
    }

    public void handleChunkFinish(char c) {
        this.queues.get(c).ack();
        this.busy = !this.queues.get(c).isFinished();
    }

    public boolean hasNextCharSet() {
        return !this.charSets.isEmpty();
    }

    public CharSet nextCharSet() throws NoSuchElementException {
        this.busy = true;
        return this.charSets.remove();
    }

    public boolean hasNextChunk() {
        boolean result = false;
        for (ChunkQueue queue : this.queues.values()) {
            result = result || !queue.isFinished() && queue.hasNext();
        }
        return result;
    }

    public PermutationChunk nextChunk() throws NoSuchElementException {
        for(ChunkQueue queue : this.queues.values()) {
            if(!queue.isFinished() && queue.hasNext()) {
                return queue.pop();
            }
        }
        throw new NoSuchElementException("No chunks available");
    }

    public boolean allAreFinished() {
        boolean result = true;
        for (ChunkQueue queue : this.queues.values()) {
            result = result && queue.isFinished();
        }
        return result;

    }

    public void add(PermutationChunk chunk) {
        this.queues.get(chunk.getMissingChar()).push(chunk);
    }

    public boolean isFinished(char c) {
        return this.queues.get(c).isFinished();
    }

    public Set<Integer> personsIncluding(char c) {
        return this.queues.get(c).includingPersons(this.personIDs);
    }

    public boolean hadHashFor(Integer personID, Character c) {
        return this.queues.get(c).hadHashFor(personID);
    }

    public static Set<Character> parseChars(String string) {
        Set<Character> set = new HashSet<>();
        for (int i = 0; i < string.length(); i++) {
            set.add(string.charAt(i));
        }
        return set;
    }

    private static Queue<CharSet> generateSubsets(Set<Character> chars) {
        Queue<CharSet> charSets = new LinkedList<>();
        for (Character c : chars) {
            Set<Character> set = new HashSet<>(chars);
            set.remove(c);
            charSets.add(new CharSet(set, c));
        }
        return charSets;
    }

    private Map<Character, ChunkQueue> initializeQueues(int batchSize, int chunkCount) {
        Map<Character, ChunkQueue> queues = new HashMap<>();
        for (CharSet charSet : this.charSets) {
            queues.put(charSet.excludedChar, new ChunkQueue(batchSize, chunkCount));
        }
        return queues;
    }

    private int getChunkCount(int charSetSize) {
        return (int) Math.ceil(CombinatoricsUtils.factorial(charSetSize) / (double) PermutationChunk.MAX_SIZE);
    }

}
