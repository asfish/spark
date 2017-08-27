package com.tryit.spark.apps.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   subtractByKey(another RDD) removes all element from the RDD for
*   which there is a key in the other RDD.
*/
public class SubtractByKeyExample
{
	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = new JavaSparkContext("local", "SubtractByKeyExample");
		// Parallelized with 3 partitions
		JavaRDD<String> x = sc.parallelize(
			Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b", "d", "c", "d", "d"), 3);

		JavaRDD<String> other = sc.parallelize(Arrays.asList("a"), 1);

		JavaSparkStatusTracker tracker = sc.statusTracker();


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

		// New JavaPairRDD with all the "a" keys filtered out
		JavaPairRDD<String, Integer> rddY = rddX.subtract(otherRdd);

		//Print tuples
		for(Tuple2<String, Integer> element : rddY.collect()){
			System.out.println("("+element._1+", "+element._2+")");
		}

		System.out.println("Active Job ID: "+tracker.getActiveJobIds());
		System.out.println("Active Stage ID: "+tracker.getActiveStageIds());

		sc.close();
	}
}
