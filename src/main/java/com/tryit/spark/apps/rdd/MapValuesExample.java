package com.tryit.spark.apps.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   mapValues(function) applies a function to each of the values in the RDD.
*/
public class MapValuesExample
{
	public static void main(String[] args) throws Exception
	{

		List<Tuple2<String, Integer>> input = new ArrayList();
		input.add(new Tuple2("coffee", 1));
		input.add(new Tuple2("coffee", 2));
		input.add(new Tuple2("coffee", 7));
		input.add(new Tuple2("coffee", 9));
		input.add(new Tuple2("pandas", 3));
		input.add(new Tuple2("pandas", 4));
		input.add(new Tuple2("pandas", 6));
		input.add(new Tuple2("pandas", 11));
		input.add(new Tuple2("pandas", 2));
		input.add(new Tuple2("pandas", 4));

		//play with the parallelism and see how it behaves.
		JavaPairRDD<String, Integer> pairRDD = Util.sc().parallelizePairs(input, 3);
		//pairRDD.partitionBy(new HashPartitioner(2));
		pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>()
		{
			@Override
			public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception
			{
				System.out.println("elements in this partition:");
				while(tuple2Iterator.hasNext())
				{
					Tuple2<String, Integer> t = tuple2Iterator.next();
					System.out.println(t._1()+" => " + t._2());
				}
			}
		});


		JavaPairRDD<String, Integer> mapValue = pairRDD.mapValues(new Function<Integer, Integer>()
		{
			@Override
			public Integer call(Integer v1) throws Exception
			{
				return v1*v1;
			}
		});

		List<Tuple2<String, Integer>> oRdd = pairRDD.collect();
		List<Tuple2<String, Integer>> mapRdd = mapValue.collect();

		System.out.println("Original rdd:");
		for(Tuple2<String,Integer> t : oRdd)
			System.out.println(t._1()+" => "+t._2());

		System.out.println("mapped rdd:");
		for(Tuple2<String,Integer> t : mapRdd)
			System.out.println(t._1()+" => "+t._2());

		//sortByKey() results in range-partitioned RDDs
		JavaPairRDD<String, Integer> sortRdd = pairRDD.sortByKey();
		System.out.println("partitioner-type: " + pairRDD.partitioner());
		System.out.println("partitioner-type: " + mapRdd);
		System.out.println("partitioner-type: " + sortRdd.partitioner());

		sortRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>()
		{
			@Override
			public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception
			{
				System.out.println("elements in this partition:");
				while(tuple2Iterator.hasNext())
				{
					Tuple2<String, Integer> t = tuple2Iterator.next();
					System.out.println(t._1()+" => " + t._2());
				}
			}
		});
	}
}
