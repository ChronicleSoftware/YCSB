package com.yahoo.ycsb.generator;

/**
 * Generate a popularity distribution of items, skewed to favor recent items significantly more than older items.
 */
public class SkewedLatestGenerator extends IntegerGenerator
{
	CounterGenerator _basis;
	ZipfianGenerator _zipfian;

	public SkewedLatestGenerator(CounterGenerator basis)
	{
		_basis=basis;
		_zipfian=new ZipfianGenerator(_basis.lastInt());
		nextInt();
	}

	/**
	 * Generate the next string in the distribution, skewed Zipfian favoring the items most recently returned by the basis generator.
	 */
	public int nextInt()
	{
		int max=_basis.lastInt();
		int nextint=max-_zipfian.nextInt(max);
		setLastInt(nextint);
		return nextint;
	}

	public static void main(String[] args)
	{
		SkewedLatestGenerator gen=new SkewedLatestGenerator(new CounterGenerator(1000));
		for (int i=0; i<Integer.parseInt(args[0]); i++)
		{
			System.out.println(gen.nextString());
		}

	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
	}

}
