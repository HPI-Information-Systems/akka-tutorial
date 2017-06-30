package de.hpi.akka_tutorial.messages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NumbersMessage implements Serializable {
	
	private static final long serialVersionUID = 1538940836039448197L;
	
	private final List<Long> numbers = new ArrayList<Long>();

	public NumbersMessage() {
	}

	public List<Long> getNumbers() {
		return this.numbers;
	}
	
	public void addNumbers(List<Long> numbers) {
		this.numbers.addAll(numbers);
	}
	
	public void addNumber(Long number) {
		this.numbers.add(number);
	}
	
	@Override
	public String toString() {
		Collections.sort(this.numbers);
		
		StringBuilder builder = new StringBuilder("Numbers[");
		for (int i = 0; i < this.numbers.size() - 1; i++)
			builder.append(this.numbers.get(i) + ",");
		builder.append(this.numbers.get(this.numbers.size() - 1) + "]");
		return builder.toString();
	}
}
