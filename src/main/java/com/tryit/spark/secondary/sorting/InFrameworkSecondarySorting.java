package com.tryit.spark.secondary.sorting;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/28/16.
 */
public class InFrameworkSecondarySorting
{
	public static void main(String[] args)
	{
		JavaSparkContext sc = new JavaSparkContext("local", "InFrameworkSecondarySorting");

		JavaRDD<String> lines = sc.parallelize(Arrays.asList( //name, time, value
			"x,2,9", "y,2,5", "x,1,3", "y,1,7", "y,3,1", "x,3,6", "z,1,4", "z,2,8", "z,3,7", "z,4,0", "p,2,6", "p,4,7",
			"p,1,9", "p,6,0", "p,7,3"
		));

		JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(
			new PairFunction<String, String, Tuple2<Integer, Integer>>()
			{
				@Override
				public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception
				{
					String[] tokens = s.split(",");
					System.out.println(tokens[0]+","+tokens[1]+","+tokens[2]);
					Tuple2<Integer,Integer> timeValue = new Tuple2<>(new Integer(tokens[1]),new Integer(tokens[2]));

					//Create composite key -> value-key conversion
					return new Tuple2<>(tokens[0]+tokens[1], timeValue);
				}
			});
		List<Tuple2<String, Tuple2<Integer,Integer>>> pairsCollection = pairs.collect();
		for(Tuple2<String, Tuple2<Integer,Integer>> t:pairsCollection)
		{
			Tuple2<Integer,Integer> interT = t._2();
			System.out.println(t._1()+" => "+interT._1()+":"+interT._2());
		}

		//in-framework sorting
		JavaPairRDD<String, Tuple2<Integer, Integer>> sortedPairs =
			pairs.repartitionAndSortWithinPartitions(new CustomPartitioner(2),new CustomComparator());
		JavaPairRDD<String, Tuple2<Integer, Integer>> sortedMap = sortedPairs.mapToPair(
			new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Tuple2<Integer, Integer>>()
			{
				@Override
				public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception
				{
					Tuple2<Integer, Integer> values = new Tuple2<Integer, Integer>(tuple2._2()._1(), tuple2._2()._2());
					return new Tuple2<>(tuple2._1().substring(0,1), values);
				}
			});

		JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = sortedMap.groupByKey();
		List<Tuple2<String,Iterable<Tuple2<Integer, Integer>>>> groupsCollection = groups.collect();
		System.out.println("Group-By-Key results");
		for(Tuple2<String,Iterable<Tuple2<Integer, Integer>>> t2 : groupsCollection)
		{
			System.out.println(t2._1()+":"+ t2._2());
			System.out.println("----------------------------");
		}

		System.out.println("Sorted-Rdd result:");
		for(Tuple2<String,Iterable<Tuple2<Integer, Integer>>> t:groupsCollection)
		{
			System.out.println(t._1()+":");
			for(Tuple2<Integer, Integer> t3: t._2())
			{
				System.out.println(t3._1()+" => "+t3._2());
			}
			System.out.println("----------------------------");
		}

		sc.close();
	}

	public static class CustomPartitioner extends Partitioner implements Serializable
	{
		private static final long serialVersionUID = 1L;
		private int partitions;

		public CustomPartitioner(int noOfPartitioners){
			partitions=noOfPartitioners;
		}

		@Override
		public int getPartition(Object key) {
			return 1;
		}

		@Override
		public int numPartitions() {
			return partitions;
		}
	}

	public static class CustomComparator implements Comparator<String>,Serializable{

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(String o1, String o2)
		{
			return o1.compareTo(o2);
		}
	}
}
