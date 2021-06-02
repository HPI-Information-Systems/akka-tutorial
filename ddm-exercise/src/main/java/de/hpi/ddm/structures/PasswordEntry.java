package de.hpi.ddm.structures;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor @NoArgsConstructor
public class PasswordEntry {
    @Getter
    private int id;
    @Getter
    private String name;
    @Getter
    private String passwordChars;
    @Getter
    private int passwordLength;
    @Getter
    private String passwordHash;
    @Getter
    private List<String> hintHashes;

    public static PasswordEntry parseFromLine(String[] line) {
        int id = Integer.parseInt(line[0]);
        String name = line[1];
        String passwordChars = line[2];
        int passwordLength = Integer.parseInt(line[3]);
        String passwordHash = line[4];
        List<String> hintHashes = Arrays.stream(line, 5, line.length).collect(Collectors.toList());
        return new PasswordEntry(id, name, passwordChars, passwordLength, passwordHash, hintHashes);
    }
}
