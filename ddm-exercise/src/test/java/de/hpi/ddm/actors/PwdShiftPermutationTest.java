package de.hpi.ddm.actors;

import org.junit.Test;



import static org.junit.Assert.*;

public class PwdShiftPermutationTest {

	@Test
	public void testSimplePwdShift() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{0,0,0,0,0};
		boolean didNotOverflow = Worker.shiftPwdPermutation(alphabetIndices, alphabet.length);
		assertArrayEquals("The first shift should simply increase the last index.", new int[]{0,0,0,0,1}, alphabetIndices);
		assertTrue("The shift should return that the generated shift did not overflow", didNotOverflow);
	}

	@Test
	public void testSimpleOverflowPwdShift() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{0,0,0,0,4};
		boolean didNotOverflow = Worker.shiftPwdPermutation(alphabetIndices, alphabet.length);
		assertArrayEquals("The shift should lead to an simple overflow.", new int[]{0,0,0,1,0}, alphabetIndices);
		assertTrue("The shift should return that the generated shift did not overflow", didNotOverflow);
	}

	@Test
	public void testComplexOverflowPwdShift() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{2,4,4,3,4};
		boolean didNotOverflow = Worker.shiftPwdPermutation(alphabetIndices, alphabet.length);
		assertArrayEquals("The shift should lead to a more complex overflow.", new int[]{2,4,4,4,0}, alphabetIndices);
		assertTrue("The shift should return that the generated shift did not overflow", didNotOverflow);
	}

	@Test
	public void testComplexOverflowPwdShift2() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{2,4,4,4,4};
		boolean didNotOverflow = Worker.shiftPwdPermutation(alphabetIndices, alphabet.length);
		assertArrayEquals("The shift should lead to a more complex overflow.", new int[]{3,0,0,0,0}, alphabetIndices);
		assertTrue("The shift should return that the generated shift did not overflow", didNotOverflow);
	}

	@Test
	public void testComplexWithoutOverflowPwdShift() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e', 'f'};
		int[] alphabetIndices = new int[]{2,4,4,3,4};
		boolean didNotOverflow = Worker.shiftPwdPermutation(alphabetIndices, alphabet.length);
		assertArrayEquals("The shift should simply increase the last index.", new int[]{2,4,4,3,5}, alphabetIndices);
		assertTrue("The shift should return that the generated shift did not overflow", didNotOverflow);
	}

	@Test
	public void testOverflowPwdShift() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{4,4,4,4,4};
		boolean didNotOverflow = Worker.shiftPwdPermutation(alphabetIndices, alphabet.length);
		assertArrayEquals("The shift should shift all entries to 0s.",new int[]{0,0,0,0,0}, alphabetIndices);
		assertFalse("The shift should return that the generated shift did overflow", didNotOverflow);
	}

	@Test
	public void testSimplePwdAdd() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] firstAlphabetIndices = new int[]{0,0,0,0,1};
		int[] secondAlphabetIndices = new int[]{0,0,0,0,1};
		boolean didNotOverflow = Worker.addPwdPermutation(firstAlphabetIndices, secondAlphabetIndices, alphabet.length);
		assertArrayEquals("The add should add the last index correctly.",new int[]{0,0,0,0,2}, firstAlphabetIndices);
		assertTrue("The add should return that the generated add did not overflow", didNotOverflow);
	}

	@Test
	public void testComplexPwdAdd() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] firstAlphabetIndices = new int[]{2,2,4,2,3};
		int[] secondAlphabetIndices = new int[]{1,4,4,2,1};
		boolean didNotOverflow = Worker.addPwdPermutation(firstAlphabetIndices, secondAlphabetIndices, alphabet.length);
		assertArrayEquals("The add should add correctly.",new int[]{4,2,3,4,4}, firstAlphabetIndices);
		assertTrue("The add should return that the generated add did not overflow", didNotOverflow);
	}

	@Test
	public void testSimpleNumberToPermutation() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{0, 0, 0 ,0 ,0 ,0, 0};
		boolean didNotOverflow = Worker.numberToPermutation(4, alphabet.length, alphabetIndices.length,alphabetIndices);
		assertArrayEquals("The add should add correctly.",new int[]{0, 0, 0 ,0 ,0 ,0, 4}, alphabetIndices);
		assertTrue("The permutation should return that the generated did not overflow", didNotOverflow);
	}

	@Test
	public void testComplexNumberToPermutation() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{0, 0, 0 ,0 ,0 ,0, 0};
		boolean didNotOverflow = Worker.numberToPermutation(255, alphabet.length, alphabetIndices.length,alphabetIndices);
		assertArrayEquals("The add should add correctly.",new int[]{0, 0, 0 ,2 ,0 ,1, 0}, alphabetIndices);
		assertTrue("The permutation should return that the generated did not overflow", didNotOverflow);
	}

	@Test
	public void testComplexOverflowNumberToPermutation() {
		char[] alphabet = new char[]{'a', 'b', 'c', 'd', 'e'};
		int[] alphabetIndices = new int[]{0, 0, 0 ,0 ,0 ,0};
		boolean didNotOverflow = Worker.numberToPermutation(23456, alphabet.length, alphabetIndices.length,alphabetIndices);
		assertArrayEquals("The add should add correctly.",new int[]{2,2,2,3,1,1}, alphabetIndices);
		assertFalse("The permutation should return that the generated did overflow", didNotOverflow);
	}

	@Test
	public void testOneCharAlphabetNumberToPermutation() {
		char[] alphabet = new char[]{'a'};
		int[] alphabetIndices = new int[]{0, 0, 0 ,0 ,0 ,0, 0};
		boolean didNotOverflow = Worker.numberToPermutation(0, alphabet.length, alphabetIndices.length,alphabetIndices);
		assertArrayEquals("The add should add correctly.",new int[]{0, 0, 0 ,0 ,0 , 0, 0}, alphabetIndices);
		assertTrue("The permutation should return that the generated did not overflow", didNotOverflow);
	}

	@Test
	public void testOneCharAlphabetOverflowNumberToPermutation() {
		char[] alphabet = new char[]{'a'};
		int[] alphabetIndices = new int[]{0, 0, 0 ,0 ,0 ,0, 0};
		boolean didNotOverflow = Worker.numberToPermutation(5, alphabet.length, alphabetIndices.length,alphabetIndices);
		assertArrayEquals("The add should add correctly.",new int[]{0, 0, 0 ,0 ,0 , 0, 0}, alphabetIndices);
		assertFalse("The permutation should return that the generated did overflow", didNotOverflow);
	}



}
