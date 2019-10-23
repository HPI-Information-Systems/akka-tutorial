package de.hpi.ddm.structures;

import java.util.List;

import de.hpi.ddm.configuration.ConfigurationSingleton;

public class BloomFilter {

	public static int DEFAULT_SIZE = 8 * 1024 * 1024 * ConfigurationSingleton.get().getDataSize();
	
	private final BitSet bits;
	private final int size;
	
	public BloomFilter() {
		this(DEFAULT_SIZE);
	}
	
	public BloomFilter(int bitSize) {
		this(bitSize, false);
	}
	
	public BloomFilter(int bitSize, boolean randomize) {
		this.bits = new BitSet(bitSize);
		this.size = bitSize;
		
		if (randomize)
			this.bits.randomize();
	}
	
	/**
	 * Retrieves the BitSet that stores the elements of this BloomFilter
	 * @return the BitSet that stores the elements of this BloomFilter
	 */
	public BitSet getBits() {
		return this.bits;
	}
	
	/**
	 * Merge all elements of the other BloomFilter into this BloomFilter.
	 * @param other the other BloomFilter whose elements are to be added
	 */
	public void merge(BloomFilter other) {
		this.bits.or(other.getBits());
	}
	
	/**
	 * Add the element to the BloomFilter.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public boolean add(BitSet element) {
		int code = element.hashCode();
		int bucket = Math.abs(code % this.size);
		
		if (this.bits.get(bucket))
			return false;
		
		this.set(bucket);
		return true;
	}
	
	/**
	 * Adds all the elements to the BloomFilter.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public void addAll(List<BitSet> elements) {
		int[] buckets = new int[elements.size()];
		for (int i = 0; i < elements.size(); i++) {
			int code = elements.get(i).hashCode();
			buckets[i] = Math.abs(code % this.size);
		}
		this.setAll(buckets);
	}
	
	/**
	 * Test if this BloomFilter contains the element.
	 * @param element the element to be tested
	 */
	public boolean contains(BitSet element) {
		int code = element.hashCode();
		int bucket = Math.abs(code % this.size);
		
		return this.bits.get(bucket);
	}
	
	private void set(int bucket) {
		this.bits.set(bucket);
	}
	
	private void setAll(int[] buckets) {
		for (int i = 0; i < buckets.length; i++)
			this.bits.set(buckets[i]);
	}
}
