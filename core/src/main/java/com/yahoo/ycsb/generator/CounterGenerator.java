package com.yahoo.ycsb.generator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates a sequence of integers 0, 1, ...
 */
public class CounterGenerator extends IntegerGenerator
{
	final AtomicInteger counter;

	/**
	 * Create a counter that starts at countstart
	 */
	public CounterGenerator(int countstart)
	{
		counter=new AtomicInteger(countstart);
		setLastInt(counter.get()-1);
	}
	
	/**
	 * If the generator returns numeric (integer) values, return the next value as an int. Default is to return -1, which
	 * is appropriate for generators that do not return numeric values.
	 */
	public int nextInt() 
	{
		int ret = counter.getAndIncrement();
		setLastInt(ret);
		return ret;
	}
	@Override
	public int lastInt()
	{
	                return counter.get() - 1;
	}
	@Override
	public double mean() {
		throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
	}
}
