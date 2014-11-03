package com.yahoo.ycsb.generator;

/**
 * A trivial integer generator that always returns the same value.
 * 
 * @author sears
 *
 */
public class ConstantIntegerGenerator extends IntegerGenerator {
	private final int i;
	/**
	 * @param i The integer that this generator will always return.
	 */
	public ConstantIntegerGenerator(int i) {
		this.i = i;
	}

	@Override
	public int nextInt() {
		return i;
	}

	@Override
	public double mean() {
		return i;
	}

}
