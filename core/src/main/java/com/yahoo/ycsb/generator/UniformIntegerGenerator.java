package com.yahoo.ycsb.generator;

import java.util.Random;

import com.yahoo.ycsb.Utils;

/**
 * Generates integers randomly uniform from an interval.
 */
public class UniformIntegerGenerator extends IntegerGenerator 
{
	int _lb,_ub,_interval;
	
	/**
	 * Creates a generator that will return integers uniformly randomly from the interval [lb,ub] inclusive (that is, lb and ub are possible values)
	 *
	 * @param lb the lower bound (inclusive) of generated values
	 * @param ub the upper bound (inclusive) of generated values
	 */
	public UniformIntegerGenerator(int lb, int ub)
	{
		_lb=lb;
		_ub=ub;
		_interval=_ub-_lb+1;
	}
	
	@Override
	public int nextInt() 
	{
		int ret=Utils.random().nextInt(_interval)+_lb;
		setLastInt(ret);
		
		return ret;
	}

	@Override
	public double mean() {
		return ((double)((long)(_lb + (long)_ub))) / 2.0;
	}
}
