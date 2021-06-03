package de.hpi.ddm.structures;

import java.util.LinkedList;
import java.util.List;

import akka.actor.ActorRef;
import lombok.Data;

@Data
public class WorkItem {
    private boolean cracked;
    private PasswordEntry passwordEntry;
    private List<ActorRef> workersCracking;

    public WorkItem(PasswordEntry entry) {
        this.cracked = false;
        this.passwordEntry = entry;
        this.workersCracking = new LinkedList<>();
    }
}
