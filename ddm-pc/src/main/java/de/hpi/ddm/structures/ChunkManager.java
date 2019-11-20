package de.hpi.ddm.structures;

import akka.actor.ActorRef;
import lombok.AllArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@AllArgsConstructor
public class ChunkManager {

    private final ActorRef master;
    private final ActorRef worker;
    private final Character missingChar;
    private List<String> permutations;

    public void add(String string) {
        this.permutations.add(string);
        if(this.permutations.size() == PermutationChunk.MAX_SIZE) {
            this.master.tell(new PermutationChunk(this.permutations, this.missingChar), this.worker);
            this.permutations = new LinkedList<>();
        }
    }

    public void flush() {
        if(this.permutations.size() > 0) {
            this.master.tell(new PermutationChunk(this.permutations, this.missingChar), this.worker);
        }
    }

}
