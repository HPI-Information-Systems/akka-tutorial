package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor @EqualsAndHashCode
public class Hint implements Serializable {

    private static final long serialVersionUID = 1747524494334503775L;
    private Integer personID;
    private String value;

}
