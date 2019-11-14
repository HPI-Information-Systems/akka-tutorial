package de.hpi.ddm.serialization;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Chunk implements Serializable {
    private static final long serialVersionUID = -6751404838728929784L;
    private byte[] bytes;
}
