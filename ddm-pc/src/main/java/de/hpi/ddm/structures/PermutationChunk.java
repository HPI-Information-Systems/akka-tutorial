package de.hpi.ddm.structures;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@Data @NoArgsConstructor
public class PermutationChunk  implements Serializable {

    public static int MAX_SIZE = 10000;

    private static final long serialVersionUID = 5776651847415346120L;
    private Character missingChar;
    private List<String> permutations;

    public PermutationChunk(List<String> permutations, Character missingChar) {
        this.permutations = new LinkedList<>(permutations);
        this.missingChar = missingChar;
    }

}
