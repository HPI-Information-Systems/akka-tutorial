package de.hpi.akka_tutorial.messages;

import java.io.Serializable;

public class RangeMessage implements Serializable {
	
	private static final long serialVersionUID = -2902027927841444319L;
	
	private final long startNumber;
	private final long endNumber;

	public RangeMessage(final long startNumber, final long endNumber) {
		this.startNumber = startNumber;
		this.endNumber = endNumber;
	}

	public long getStartNumber() {
		return this.startNumber;
	}

	public long getEndNumber() {
		return this.endNumber;
	}

	@Override
	public String toString() {
		return "Range[" + this.startNumber + "," + this.endNumber + "]";
	}
}
