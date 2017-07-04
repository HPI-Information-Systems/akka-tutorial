package de.hpi.akka_tutorial;

import de.hpi.akka_tutorial.prime.PrimeCalculator;
import de.hpi.akka_tutorial.remote.Calculator;

public class Main {
	
	public static void main(String[] args) {
		
	//	startPrimeExample();
		
		startRemoteExample();
	}
	
	private static void startPrimeExample() {
		
		long startNumber = 0;
		long endNumber = 100;

		PrimeCalculator primeCalculator = new PrimeCalculator();
		primeCalculator.calculate(startNumber, endNumber);
	}
	
	private static void startRemoteExample() {
		
		long startNumber = 0;
		long endNumber = 100;

		Calculator calculator = new Calculator();
		calculator.calculate(startNumber, endNumber);
	}
}
