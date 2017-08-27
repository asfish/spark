package com.tryit.spark.apps.rdd;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   rightOuterJoin(another RDD) performs a right outer join between the
*   two RDDs in which all keys must be present in the other RDD.
 */
public class RightOuterJoinExample
{
	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = new JavaSparkContext("local", "RightOuterJoinExample");
		// Parallelized with 3 partitions
		JavaRDD<String> x = sc.parallelize(
			Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b", "d", "c", "d", "d"), 3);

		JavaRDD<String> other = sc.parallelize(Arrays.asList("a", "e", "f", "e", "g", "f"), 2);

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

		// do rightOuterJoin
		JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rddY_right = rddX.rightOuterJoin(otherRdd);

		System.out.println("Right outer join:");
		//Print tuples
		for(Tuple2<String, Tuple2<Optional<Integer>, Integer>> element : rddY_right.collect()){
			System.out.println("("+element._1+", "+element._2+")");
		}

		// do rightOuterJoin
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> rddY_left = rddX.leftOuterJoin(otherRdd);

		System.out.println("left outer join:");
		//Print tuples
		for(Tuple2<String, Tuple2<Integer, Optional<Integer>>> element : rddY_left.collect()){
			System.out.println("("+element._1+", "+element._2+")");
		}
		sc.close();
	}
}
