package de.hpi.ddm.structures;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class BitSet implements Cloneable, Serializable {

	private static final long serialVersionUID = 3988558361682959287L;
	
	private final static int BITSHIFTS_TO_WORD_SIZE = 6;
	private final static int BITS_PER_WORD = 1 << BITSHIFTS_TO_WORD_SIZE;
	private final static int BYTES_PER_WORD = 8;
	private final static long WORD_MASK = 0xffffffffffffffffL;
	
	private long[] words;

	/**
	 * Calculate the word index that stores the given bit index.
	 * @param bitIndex that needs to be located
	 * @return wordIndex that stores the bitIndex
	 */
	private static int wordIndex(int bitIndex) {
		return bitIndex >> BITSHIFTS_TO_WORD_SIZE; // Because 2^6=64 bits are in each word and >>6 is equivalent to /2^6
	}

	/**
	 * Construct a new {@code BitSet} with a certain capacity. All bits are initially set to {@code false}.
	 * @param capacity many bits that can be stored in the {@code BitSet}
	 * @return a new {@code BitSet} instance with the given capacity
	 * @throws NegativeArraySizeException if the specified capacity is negative
	 */
	public BitSet(int capacity) {
		this.words = new long[wordIndex(capacity - 1) + 1];
	}

	/**
	 * FOR (DE-)SERIALIZATION ONLY!
	 */
	public BitSet() {
	}
	
	/**
	 * Construct a new {@code BitSet} backed by the given worlds array. All bits are set according the the given words array.
	 * @param words that are copied as the backing words array for this new {@code BitSet}
	 * @return a new {@code BitSet} instance with the given words backing it
	 */
	public BitSet(long[] words) {
		this.words = Arrays.copyOf(words, words.length);
	}

	public static BitSet fromBinary(byte[] bytes) {
		return BitSet.fromBinary(ByteBuffer.wrap(bytes));
	}

	public static BitSet fromBinary(ByteBuffer buffer) {
		long[] words = new long[buffer.getInt()];
		for (int i = 0; i < words.length; i++)
			words[i] = buffer.getLong();
		return new BitSet(words);
	}

	public byte[] toBinary() {
		byte[] bytes = new byte[this.binarySize()];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		
		buffer.putInt(this.words.length);
		for (int i = 0; i < this.words.length; i++)
			buffer.putLong(this.words[i]);
		return bytes;
	}
	
	public void toBinary(ByteBuffer buffer) {
		buffer.putInt(this.words.length);
		for (int i = 0; i < this.words.length; i++)
			buffer.putLong(this.words[i]);
	}
	
	/**
	 * Calculate the binary size in bytes of this object when being serialized into a byte array.
	 * The byte size is a 4 byte integer to encode the number of words in this {@code BitSet} plus 8 bytes per word.
	 * @return the binary size of this object
	 */
	public int binarySize() {
		return 4 + this.words.length * BYTES_PER_WORD;
	}

	/**
	 * Flip the bit at the specified index.
	 * @param bitIndex to be flipped
	 */
	public void flip(int bitIndex) {
		int wordIndex = wordIndex(bitIndex);
		
		this.words[wordIndex] ^= (1L << bitIndex);
	}

	/**
	 * Set the bit at the specified index to {@code true}.
	 * @param bitIndex to be set
	 */
	public void set(int bitIndex) {
		int wordIndex = wordIndex(bitIndex);
		
		this.words[wordIndex] |= (1L << bitIndex);
	}

	/**
	 * Set the bit at the specified index to the specified value.
	 * @param bitIndex to be set
	 * @param value to set the bit to
	 */
	public void set(int bitIndex, boolean value) {
		if (value)
			set(bitIndex);
		else
			clear(bitIndex);
	}

	/**
	 * Set all bits from the specified {@code fromIndex} (inclusive) to the specified {@code toIndex} (exclusive) to {@code true}.
	 * @param fromIndex is the index of the first bit to be set
	 * @param toIndex is the index after the last bit to be set
	 */
	public void set(int fromIndex, int toIndex) {
		if (fromIndex == toIndex)
			return;

		int startWordIndex = wordIndex(fromIndex);
		int endWordIndex = wordIndex(toIndex - 1);
		
		long firstWordMask = WORD_MASK << fromIndex;
		long lastWordMask = WORD_MASK >>> -toIndex;
		
		if (startWordIndex == endWordIndex) {
			this.words[startWordIndex] |= (firstWordMask & lastWordMask);
			return;
		} 
		
		this.words[startWordIndex] |= firstWordMask;
		for (int i = startWordIndex + 1; i < endWordIndex; i++)
			this.words[i] = WORD_MASK;
		this.words[endWordIndex] |= lastWordMask;
	}

	/**
	 * Set the bit at the specified index to {@code false}.
	 * @param bitIndex to be cleared
	 */
	public void clear(int bitIndex) {
		int wordIndex = wordIndex(bitIndex);
		
		this.words[wordIndex] &= ~(1L << bitIndex);
	}

	/**
	 * Set all the bits in this BitSet to {@code false}.
	 */
	public void clear() {
		for (int i = 0; i < this.words.length; i++)
			this.words[i] = 0;
	}

	/**
	 * Return the value of the bit at the specified index. 
	 * The value is {@code true} if the bit with the index {@code bitIndex} is set; otherwise, the result is {@code false}.
	 * @param bitIndex to be checked
	 * @return the value of the bit with the specified index
	 */
	public boolean get(int bitIndex) {
		int wordIndex = wordIndex(bitIndex);
		
		return (this.words[wordIndex] & (1L << bitIndex)) != 0;
	}

	/**
	 * Returns the index of the first bit that is set to {@code true} that occurs at or after the specified starting index. 
	 * If no such bit exists then {@code -1} is returned.
	 *
	 * <p>
	 * To iterate over the {@code true} bits in a {@code BitSet}, use the following loop:
	 *
	 * <pre>
	 *  {@code
	 * for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
	 *     // operate on index i here
	 *     if (i == Integer.MAX_VALUE) {
	 *         break; // or (i+1) would overflow
	 *     }
	 * }}
	 * </pre>
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the next set bit, or {@code -1} if there is no such bit
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public int nextSetBit(int fromIndex) {
		int wordIndex = wordIndex(fromIndex);
		
		if (wordIndex >= this.words.length)
			return -1;

		long word = this.words[wordIndex] & (WORD_MASK << fromIndex);

		while (true) {
			if (word != 0)
				return (wordIndex * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
			if (++wordIndex == this.words.length)
				return -1;
			word = this.words[wordIndex];
		}
	}

	/**
	 * Returns the index of the first bit that is set to {@code false} that occurs at or after the specified starting index.
	 * If no such bit exists then {@code -1} is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the next clear bit, or {@code -1} if there is no such bit
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public int nextClearBit(int fromIndex) {
		int wordIndex = wordIndex(fromIndex);
		
		if (wordIndex >= this.words.length)
			return -1;

		long word = ~this.words[wordIndex] & (WORD_MASK << fromIndex);

		while (true) {
			if (word != 0)
				return (wordIndex * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
			if (++wordIndex == this.words.length)
				return -1;
			word = ~this.words[wordIndex];
		}
	}

	/**
	 * Returns the index of the first bit that is set to {@code true} that occurs at or before the specified starting index. 
	 * If no such bit exists then {@code -1} is returned.
	 *
	 * <p>
	 * To iterate over the {@code true} bits in a {@code BitSet}, use the following loop:
	 *
	 * <pre>
	 *  {@code
	 * for (int i = bs.length(); (i = bs.previousSetBit(i-1)) >= 0; ) {
	 *     // operate on index i here
	 * }}
	 * </pre>
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the previous set bit, or {@code -1} if there is no such bit
	 */
	public int previousSetBit(int fromIndex) {
		if (fromIndex < 0)
			return -1;
		
		int wordIndex = wordIndex(fromIndex);
		
		if (wordIndex >= this.words.length)
			return this.logicalLength() - 1;

		long word = this.words[wordIndex] & (WORD_MASK >>> -(fromIndex + 1));

		while (true) {
			if (word != 0)
				return (wordIndex + 1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
			if (wordIndex-- == 0)
				return -1;
			word = this.words[wordIndex];
		}
	}

	/**
	 * Returns the index of the first bit that is set to {@code false} that occurs at or before the specified starting index.
	 * If no such bit exists then {@code -1} is returned.
	 * 
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the previous clear bit, or {@code -1} if there is no such bit
	 */
	public int previousClearBit(int fromIndex) {
		if (fromIndex < 0)
			return -1;

		int wordIndex = wordIndex(fromIndex);
		
		if (wordIndex >= this.words.length)
			return fromIndex;

		long word = ~this.words[wordIndex] & (WORD_MASK >>> -(fromIndex + 1));

		while (true) {
			if (word != 0)
				return (wordIndex + 1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
			if (wordIndex-- == 0)
				return -1;
			word = ~this.words[wordIndex];
		}
	}

	/**
	 * Calculate the "logical size" of this {@code BitSet}, i.e., the index of the highest set bit in the {@code BitSet} plus one. 
	 * If the {@code BitSet} contains no set bits then {@code 0} is returned.
	 * @return the logical size of this {@code BitSet}
	 */
	public int logicalLength() {
		int length = BITS_PER_WORD * this.words.length;
		for (int i = this.words.length - 1; i >= 0; i--) {
			length -= BITS_PER_WORD;
			
			if (this.words[i] != 0)
				return length + BITS_PER_WORD - Long.numberOfLeadingZeros(this.words[i]);
		}
		return length;
	}

	/**
	 * Calculate the "physical length" of this {@code BitSet}, which is the length of the internal words array.
	 * @return length of the internal words array
	 */
	public int physicalLength() {
		return this.words.length;
	}
	
	/**
	 * Calculate the number of bits set to {@code true} in this {@code BitSet}.
	 * @return the number of bits set to {@code true} in this {@code BitSet}
	 */
	public int cardinality() {
		int cardinality = 0;
		for (int i = 0; i < this.words.length; i++)
			cardinality += Long.bitCount(this.words[i]);
		return cardinality;
	}

	/**
	 * Return true if this {@code BitSet} contains no bits that are set to {@code true}.
	 * @return {@code true} if this {@code BitSet} is empty; {@code false} otherwise
	 */
	public boolean isEmpty() {
		return this.cardinality() == 0;
	}

	private void ensureCompatibility(BitSet set) {
		if (this.words.length != set.words.length)
			throw new RuntimeException(BitSet.class.getName() + " can operate only with " + BitSet.class.getName() + "s of same size.\n This words length was " + this.words.length + " while the other's word length was " + set.words.length + ".");
	}
	
	/**
	 * Return {@code true} if the specified {@code BitSet} has any bits set to {@code true} that are also set to {@code true} in this {@code BitSet}.
	 * @param set is the {@code BitSet} to intersect with
	 * @return {@code true} if this {@code BitSet} intersects with the specified {@code BitSet}
	 */
	public boolean intersects(BitSet set) {
		this.ensureCompatibility(set);
		if (this == set)
			return true;
		
		for (int i = 0; i < this.words.length; i++)
			if ((this.words[i] & set.words[i]) != 0)
				return true;
		return false;
	}

	/**
	 * Perform a logical <b>AND</b> of this target {@code BitSet} with the argument {@code BitSet}. 
	 * This {@code BitSet} is modified so that each bit in it has the value {@code true} if it initially was {@code true} and its corresponding bit in the specified {@code BitSet} is {@code true}.
	 * @param set is the {@code BitSet} to perform the and-operation with
	 */
	public void and(BitSet set) {
		this.ensureCompatibility(set);
		if (this == set)
			return;

		for (int i = 0; i < this.words.length; i++)
			this.words[i] &= set.words[i];
	}

	/**
	 * Perform a logical <b>OR</b> of this target {@code BitSet} with the argument {@code BitSet}.
	 * This {@code BitSet} is modified so that each bit in it has the value {@code true} if it initially was {@code true} or its corresponding bit in the specified {@code BitSet} is {@code true}.
	 * @param set is the {@code BitSet} to perform the or-operation with
	 */
	public void or(BitSet set) {
		this.ensureCompatibility(set);
		if (this == set)
			return;

		for (int i = 0; i < this.words.length; i++)
			this.words[i] |= set.words[i];
	}

	/**
	 * Perform a logical <b>XOR</b> of this target {@code BitSet} with the argument {@code BitSet}.
	 * This {@code BitSet} is modified so that each bit in it has the value {@code true} if either its initial value or the value of the corresponding bit in the specified {@code BitSet} was {@code true} and one of the two bits was {@code false}.
	 * @param set is the {@code BitSet} to perform the xor-operation with
	 */
	public void xor(BitSet set) {
		this.ensureCompatibility(set);
		if (this == set) {
			this.clear();
			return;
		}

		for (int i = 0; i < this.words.length; i++)
			this.words[i] ^= set.words[i];
	}

	/**
	 * Remove all the bits in this {@code BitSet} whose corresponding bit is also set in the specified {@code BitSet}.
	 * @param set is the {@code BitSet} whose bits are to be removed from this {@code BitSet}
	 */
	public void andNot(BitSet set) {
		this.ensureCompatibility(set);
		if (this == set) {
			this.clear();
			return;
		}

		for (int i = 0; i < this.words.length; i++)
			this.words[i] &= ~set.words[i];
	}

	public void randomize() {
		Random rand = new Random();
		for (int i = 0; i < this.words.length; i++)
			this.words[i] = rand.nextLong();
	}
	
	@Override
	public int hashCode() {
		long h = 1234;
		for (int i = this.words.length - 1; i >= 0; i--)
			h ^= this.words[i] * (i + 1);

		return (int) ((h >> 32) ^ h);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BitSet))
			return false;
		if (this == obj)
			return true;

		BitSet set = (BitSet) obj;

		if (this.words.length != set.words.length)
			return false;

		for (int i = 0; i < this.words.length; i++)
			if (this.words[i] != set.words[i])
				return false;

		return true;
	}

	@Override
	public BitSet clone() {
		return new BitSet(this.words);
	}
	
	public String toString() {
		int numBits = (this.words.length > 128) ? cardinality() : this.words.length * BITS_PER_WORD;
		StringBuilder b = new StringBuilder(6 * numBits + 2);
		b.append('{');

		int i = nextSetBit(0);
		if (i != -1) {
			b.append(i);
			while (true) {
				if (++i < 0)
					break;
				if ((i = nextSetBit(i)) < 0)
					break;
				int endOfRun = nextClearBit(i);
				do {
					b.append(", ").append(i);
				} while (++i != endOfRun);
			}
		}

		b.append('}');
		return b.toString();
	}
}
