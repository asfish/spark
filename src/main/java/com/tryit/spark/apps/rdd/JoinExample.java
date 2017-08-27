package com.tryit.spark.apps.rdd;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   join(otherDataset, [numTasks])
*   When called on datasets of type (K, V) and (K, W), returns a
*   dataset of (K, (V, W)) pairs with all pairs of elements for
*   each key. Outer joins are supported through leftOuterJoin,
*   rightOuterJoin, and fullOuterJoin.
 */
public class JoinExample
{
	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = new JavaSparkContext("local", "JoinExample");
		// Parallelized with 3 partitions
		JavaRDD<String> x = sc.parallelize(
			Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b", "d", "c", "d", "d"), 3);

		JavaRDD<String> other = sc.parallelize(Arrays.asList("a"), 1);

		System.out.println("number of partitions for rdd x: " + x.partitions().size());

		// PairRDD parallelized with 3 partitions
		// mapToPair function will map JavaRDD to JavaPairRDD
		JavaPairRDD<String, Integer> rddX =
			x.mapToPair(new PairFunction<String, String, Integer>()
			{
				@Override
				public Tuple2<String, Integer> call(String s) throws Exception
				{
					return new Tuple2<String, Integer>(s,1);
				}
			});

		JavaPairRDD<String, Integer> otherRdd =
			other.mapToPair(new PairFunction<String, String, Integer>()
			{
				@Override
				public Tuple2<String, Integer> call(String s) throws Exception
				{
					return new Tuple2<>(s,1);
				}
			});

		// do joins --> same as inner join
		JavaPairRDD<String, Tuple2<Integer, Integer>> rddY = rddX.join(otherRdd);

		//Print tuples
		for(Tuple2<String, Tuple2<Integer, Integer>> element : rddY.collect()){
			System.out.println("("+element._1+", "+element._2+")");
		}
		sc.close();
	}
}
