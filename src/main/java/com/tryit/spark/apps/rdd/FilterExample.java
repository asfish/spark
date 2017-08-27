package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 * http://backtobazics.com/big-data/spark/apache-spark-filter-example/
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FilterExample
{
	public static void main(String[] args) throws Exception
	{
		// Parallelized with 2 partitions
		JavaRDD<Integer> rddX = Util.sc().parallelize(
			Arrays.asList(1, 9, 2, 7, 3,0, 4, 2, 5,3, 6, 12, 7,14, 8,20, 9, 15,10),
			3);

		// filter operation will return List of Array in following case
		JavaRDD<Integer> rddY = rddX.filter(new Function<Integer, Boolean>()
		{
			@Override
			public Boolean call(Integer v1) throws Exception
			{
				//Filter Predicate
				return v1 % 2 == 0;
			}
		});

		List<Integer> filteredList = rddY.collect();

		System.out.println("original list: " + rddX.collect());
		System.out.println("filtered list: " + filteredList);

		//=========== mapPartitionsWithIndex=========================================
		JavaRDD<Tuple2<Integer, List<Integer>>> mapPartitionRdd = rddX.mapPartitionsWithIndex(
			new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, List<Integer>>>>()
			{
				List<Tuple2<Integer, List<Integer>>> tls = new ArrayList<>();

				@Override
				public Iterator<Tuple2<Integer, List<Integer>>> call(Integer v1, Iterator<Integer> v2) throws Exception
				{
					List<Integer> ls = new ArrayList<>();
					Tuple2<Integer, List<Integer>> t = new Tuple2<>(v1,ls);
					while(v2.hasNext())
					{
						ls.add(v2.next());
					}
					tls.add(t);
					return tls.iterator();
				}
			}, false);

		System.out.println("mapPartitionRdd list: " + mapPartitionRdd.collect());

		System.out.println("The execution plan for mapPartitionRdd:\n" + mapPartitionRdd.toDebugString());

		JavaPairRDD<Integer, List<Integer>> pairRDD = mapPartitionRdd.mapToPair(
			new PairFunction<Tuple2<Integer, List<Integer>>, Integer, List<Integer>>()
		{
			@Override
			public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, List<Integer>> integerListTuple2) throws Exception
			{
				return new Tuple2<>(integerListTuple2._1(), integerListTuple2._2());
			}
		});

		List<Tuple2<Integer, List<Integer>>> list = pairRDD.collect();
		System.out.println("PairRDD results: ");
		for(Tuple2<Integer, List<Integer>> t : list)
			System.out.println(t._1()+" => "+t._2());

/*
		JavaPairRDD<Integer, List<Integer>> pairPartitionRdd = rddX.mapPartitionsToPair(
			new PairFlatMapFunction<Iterator<Integer>, Integer, List<Integer>>()
			{
				List<Integer> ints = new ArrayList<>();

				@Override
				public Iterator<Tuple2<Integer, List<Integer>>> call(Iterator<Integer> integerIterator) throws Exception
				{
					while(integerIterator.hasNext())
						ints.add(integerIterator.next());
					//return new Tuple2<Integer, List<Integer>>(1, ints);
					return new ArrayList<Tuple2<Integer, List<Integer>>>().iterator();
				}
			});
		pairPartitionRdd.collect();
		*/
	}
}