package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 * http://backtobazics.com/big-data/spark/apache-spark-groupby-example/
 *
 * groupBy() works on unpaired data or data where we want to use a different
 * condition besides equality on the current key.
 */
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class GroupByExample
{
	public static void main(String[] args) throws Exception
	{


		// Parallelized with 3 partitions
		JavaRDD<String> rddX = Util.sc().parallelize(
			Arrays.asList("Joseph", "Jimmy", "Tina",
				"Thomas", "James", "Cory",
				"Christine", "Jackeline", "Juan"), 3);

		JavaPairRDD<Character, Iterable<String>> rddY = rddX.groupBy(
			new Function<String, Character>()
			{
				@Override
				public Character call(String word) throws Exception
				{
					return word.charAt(0);
				}
			});

		/*rddY.groupBy(new Function<Tuple2<Character, Iterable<String>>, Object>()
		{
			@Override
			public Object call(Tuple2<Character, Iterable<String>> v1) throws Exception
			{
				return null;
			}
		});*/

		System.out.println(rddY.collect());
	}
}
