package de.hpi.ddm.structures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import akka.actor.ActorRef;
import de.hpi.ddm.actors.Worker.WorkMessage;
import lombok.Data;

@Data
public class CrackingState {
    private WorkMessage workMessage;
    private ActorRef workItemSender;
    private List<String> hintHashes;
    private int crackedHintsCounter;
    private List<Character> remainingUniquePasswordCharsList;
    private Iterator<Character> remainingUniquePasswordCharsIterator;
    private LinkedList<String> uniquePasswordCharCombs;
    private Iterator<String> uniquePasswordCharCombsIterator;

    // The following two variables are per iteration of a combination of passwords.
    private LinkedList<String> possiblePasswords;
    private Iterator<String> possiblePasswordsIterator;

    public CrackingState(WorkMessage message, ActorRef workItemSender) {
        this.workMessage = message;
        this.workItemSender = workItemSender;
        
        this.crackedHintsCounter = 0;

        PasswordEntry passwordEntry = message.getPasswordEntry();
        this.hintHashes = new ArrayList<>(passwordEntry.getHintHashes());
        this.remainingUniquePasswordCharsList = passwordEntry.getPasswordChars().chars()
            .mapToObj(c -> (char) c)
            .collect(Collectors.toList());
        this.remainingUniquePasswordCharsIterator = remainingUniquePasswordCharsList.iterator();

        this.uniquePasswordCharCombs = new LinkedList<>();
    }

}
