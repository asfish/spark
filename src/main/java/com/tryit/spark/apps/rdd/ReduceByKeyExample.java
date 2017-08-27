package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 */
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReduceByKeyExample
{

	public static void main(String[] args) throws Exception {
		// Parallelized with 3 partitions
		JavaRDD<String> x = Util.sc().parallelize(
			Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b"),
			3);
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

		// New JavaPairRDD
		JavaPairRDD<String, Integer> rddY = rddX.reduceByKey(
			new Function2<Integer, Integer, Integer>()
			{
				@Override
				public Integer call(Integer v1, Integer v2) throws Exception
				{
					//Reduce Function for sum
					return v1 + v2;
				}
			}
		);

		//Print tuples
		for(Tuple2<String, Integer> element : rddY.collect()){
			System.out.println("("+element._1+", "+element._2+")");
		}
	}
}
