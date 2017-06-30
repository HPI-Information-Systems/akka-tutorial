package de.hpi.akka_tutorial;

public class Main {
	
	public static void main(String[] args) {
		
		long startNumber = 0;
		long endNumber = 100;

		PrimeCalculator primeCalculator = new PrimeCalculator();
		primeCalculator.calculate(startNumber, endNumber);
	}
}
