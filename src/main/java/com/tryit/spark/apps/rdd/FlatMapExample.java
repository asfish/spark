package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 * http://backtobazics.com/big-data/spark/apache-spark-flatmap-example/
 */
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.mortbay.util.SingletonList;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import scala.Tuple2;

public class FlatMapExample
{
	public static void main(String[] args) throws Exception
	{
		// Parallelized with 2 partitions
		JavaRDD<String> rddX = Util.sc().parallelize(
			Arrays.asList("spark rdd example is very helpful", "sample example is for teaching you"),
			2);

		// map operation will return List of Array in following case
		JavaRDD<String[]> mapRDD = rddX.map(
			new Function<String, String[]>()
			{
				@Override
				public String[] call(String v1) throws Exception
				{
					return v1.split(" ");
				}
			});

		List<String[]> listUsingMap = mapRDD.collect();
		for(String[] strings : listUsingMap)
		{
			System.out.println("mapped listUsingMap:");
			for (String s : strings)
				System.out.println(s);
		}
		// flatMap operation will return list of String in following case
		JavaRDD<String> flatMapRDD = rddX.flatMap(
			new FlatMapFunction<String, String>()
			{
				@Override
				public Iterator<String> call(String s) throws Exception
				{
					return Arrays.asList(s.split(" ")).iterator();
				}
			});
		List<String> listUsingFlatMap = flatMapRDD.collect();
		System.out.println("flat mapped flatMapRDD: " + listUsingFlatMap);


		//create pair rdd from flat map rdd
		JavaPairRDD<String, Integer> maToPairRDD = flatMapRDD.mapToPair(
			new PairFunction<String, String, Integer>()
			{
				@Override
				public Tuple2<String, Integer> call(String s) throws Exception
				{
					return new Tuple2(s, 1);
				}
			});

		Optional<org.apache.spark.Partitioner> partitioner = maToPairRDD.partitioner();
		//org.apache.spark.Partitioner p = partitioner.get();
		if(!partitioner.isPresent())
			System.out.println("this pairRdd does not have partitioner");

		List<Tuple2<String, Integer>> lsT2 = maToPairRDD.collect();
		System.out.println("map-to-pair => pairRDD:");
		for (Tuple2<String, Integer> t2: lsT2)
			System.out.println(t2._1() + ":" + t2._2());


		JavaPairRDD<String, Integer> mapValueRDD = maToPairRDD.mapValues(
			new Function<Integer, Integer>()
			{
				@Override
				public Integer call(Integer v1) throws Exception
				{
					return v1+v1;
				}
			});
		System.out.println("map values - mapValueRDD:");
		for (Tuple2<String, Integer> t2: mapValueRDD.collect())
			System.out.println(t2._1() + "=>" + t2._2());


		JavaPairRDD<String, String> flatMapValueRDD = maToPairRDD.flatMapValues(
			new Function<Integer, Iterable<String>>()
			{
				@Override
				public Iterable<String> call(Integer v1) throws Exception
				{
					//it.unimi.dsi.fastutil.objects.Object2ObjectMap<String, Object>
					//return SingletonList.newSingletonList(v1.toString());
					return Collections.singleton(v1.toString());
				}
			});
		System.out.println("flat map values - flatMapValueRDD: " +flatMapValueRDD.collect().size());
		for (Tuple2<String, String> t2: flatMapValueRDD.collect())
			System.out.println(t2._1() + "=>"+ t2._2());
	}
}
