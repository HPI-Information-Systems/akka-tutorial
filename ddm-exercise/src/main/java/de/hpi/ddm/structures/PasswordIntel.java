package de.hpi.ddm.structures;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @AllArgsConstructor
public class PasswordIntel implements Serializable {

	private static final long serialVersionUID = 1704338361682959287L;
	
    private String alphabet;
    private String user;
    private String pwdHash;
    private int pwdLength;
	private String[] hintHashes;
    private int uncrackedHashCounter; 
    private Set<Char> knownFalseChars;
    
    public PasswordIntel(String[] csvLine) {
        this.user = csvLine[1];
        this.alphabet = csvLine[2];
        this.pwdLength = Integer.parseInt(csvLine[3]);
        this.pwdHash = csvLine[4];
        this.hintHashes = Arrays.copyOfRange(csvLine, 5, csvLine.length - 1);
        this.uncrackedHashCounter = this.hintHashes.length;
        this.knownFalseChars = new Set<Char>();
    }
}
