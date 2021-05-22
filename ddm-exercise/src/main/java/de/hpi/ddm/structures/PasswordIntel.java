package de.hpi.ddm.structures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import lombok.AllArgsConstructor;
import lombok.Data;
import scala.Char;

@Data @AllArgsConstructor
public class PasswordIntel implements Serializable {

	private static final long serialVersionUID = 1704338361682959287L;
	
    private String alphabet;
    private String user;
    private String pwdHash;
    private int pwdLength;
	private String[] hintHashes;
    private int uncrackedHashCounter; 
    private HashSet<Character> knownFalseChars;
    
    public PasswordIntel(String[] csvLine) {
        this.user = csvLine[1];
        this.alphabet = csvLine[2];
        this.pwdLength = Integer.parseInt(csvLine[3]);
        this.pwdHash = csvLine[4];
        this.hintHashes = Arrays.copyOfRange(csvLine, 5, csvLine.length - 1);
        this.uncrackedHashCounter = this.hintHashes.length;
        this.knownFalseChars = new HashSet<Character>();
    }

    public void addFalseChar(String subAlphabetWithKnownHash){
        int missing =  -1;
        for(int i = 0; i < alphabet.length(); ++i){
            boolean found = false;
            for(int j = 0; j < subAlphabetWithKnownHash.length(); ++j){
                char b = subAlphabetWithKnownHash.charAt(j);
                if(alphabet.charAt(i) == b){
                    found = true;
                }
            }
            if(!found){
                missing = i;
                break;
            }
        }
        this.knownFalseChars.add(alphabet.charAt(missing));
    }
}
