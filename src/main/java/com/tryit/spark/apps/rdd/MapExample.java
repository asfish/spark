package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 */
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MapExample
{

	public static void main(String[] args) throws Exception {

		// Parallelized with 2 partitions
		JavaRDD<String> baseRdd = Util.sc().parallelize(
			Arrays.asList("spark", "rdd", "example", "sample", "example"),
			2);
		System.out.println("baseRdd: ");
		for(String ls : baseRdd.collect())
			System.out.println(ls);

		// Word Count Map Example
		JavaRDD<Tuple2<String, Integer>> mappedRdd = baseRdd.map(new Function<String, Tuple2<String, Integer>>()
		{
			@Override
			public Tuple2<String, Integer> call(String v1) throws Exception
			{
				return new Tuple2<>(v1, 1);
			}
		});

		List<Tuple2<String, Integer>> list1 = mappedRdd.collect();
		System.out.println("list1-rdd type: " + mappedRdd);
		System.out.println("mappedRdd: ");
		for(Tuple2<String, Integer> t2 : list1)
			System.out.println(t2._1()+ ":" + t2._2());

		// Another example of making tuple with string and it's length
		JavaRDD<Tuple2<String, Integer>> mappedRdd2 = baseRdd.map(
			new Function<String, Tuple2<String, Integer>>()
			{
				@Override
				public Tuple2<String, Integer> call(String v1) throws Exception
				{
					return new Tuple2<>(v1, v1.length());
				}
			});
		System.out.println("list1-rdd type: " + mappedRdd2);
		List<Tuple2<String, Integer>> list2 = mappedRdd2.collect();
		System.out.println("mappedRdd2: ");
		for(Tuple2<String, Integer> t2 : list2)
			System.out.println(t2._1()+ ":" + t2._2());
	}
}
