package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 * http://backtobazics.com/big-data/spark/apache-spark-reduce-example/
 */
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

public class ReduceExample
{
	public static void main(String[] args) throws Exception
	{
		// Parallelized with 2 partitions
		JavaRDD<Integer> rddX = Util.sc().parallelize(
			Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

		// cumulative sum
		Integer cSum = rddX.reduce(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer accum, Integer num) throws Exception
			{
				//Reduce Function for cumulative sum
				return accum + num;
			}
		});

		// another way to write
		//Integer cSumInline = rddX.reduce((accum, n) -> (accum + n));

		// cumulative multiplication
		Integer cMul = rddX.reduce(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer accum, Integer n) throws Exception
			{
				//Reduce Function for cumulative multiplication
				return accum * n;
			}
		});
		// another way to write
		//Integer cMulInline = rddX.reduce((accum, n) -> (accum * n));

		System.out.println("cSum: " + cSum + "\ncMul: " + cMul );
	}
}
