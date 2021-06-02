package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class WorkItem {
    private boolean working;
    private String[] inputLine;

    public WorkItem(String[] input) {
        this.inputLine = input;
    }
}
