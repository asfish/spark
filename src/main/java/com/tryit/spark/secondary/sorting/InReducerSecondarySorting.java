package com.tryit.spark.secondary.sorting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/28/16.
 */
public class InReducerSecondarySorting
{
	public static void main(String[] args)
	{
		JavaSparkContext sc = new JavaSparkContext("local", "InReducerSecondarySorting");

		JavaRDD<String> lines = sc.parallelize(Arrays.asList( //name, time, value
			"x,2,9", "y,2,5", "x,1,3", "y,1,7", "y,3,1","x,3,6", "z,1,4", "z,2,8","z,3,7", "z,4,0","p,2,6","p,4,7",
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

					return new Tuple2<>(tokens[0], timeValue);
				}
			});
		List<Tuple2<String, Tuple2<Integer,Integer>>> pairsCollection = pairs.collect();
		for(Tuple2<String, Tuple2<Integer,Integer>> t:pairsCollection)
		{
			Tuple2<Integer,Integer> interT = t._2();
			System.out.println(t._1()+" => "+interT._1()+":"+interT._2());
		}

		JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();
		List<Tuple2<String,Iterable<Tuple2<Integer, Integer>>>> groupsCollection = groups.collect();
		System.out.println("Group-By-Key results");
		for(Tuple2<String,Iterable<Tuple2<Integer, Integer>>> t2 : groupsCollection)
		{
			System.out.println(t2._1()+":"+ t2._2());
			/*for(Tuple2<Integer, Integer> t3: t2._2())
			{
				//Tuple2<Integer, Integer> innerList = t2._2().iterator().next();
				System.out.println(t3._1()+" => "+t3._2());
			}*/
			System.out.println("----------------------------");
		}

		//in-reducer sorting: Sort the reducer's values, to get the final values
		JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(
			new Function<Iterable<Tuple2<Integer, Integer>>, //input
				Iterable<Tuple2<Integer, Integer>>             //ouput
				>(){

				@Override
				public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception
				{
					List<Tuple2<Integer, Integer>> list = new ArrayList<>();
					CollectionUtils.addAll(list, v1.iterator());
					Collections.sort(list, new Comparator<Tuple2<Integer, Integer>>()
					{
						@Override
						public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2)
						{
							//sort by value
							//return t1._2()-t2._2();

							//sort by time
							return t1._1()-t2._1();
						}
					});//new TupleComparator());

					return list;
				}
			});

		List<Tuple2<String,Iterable<Tuple2<Integer, Integer>>>> sortedCollections = sorted.collect();
		System.out.println("Sorted-Rdd result:");
		for(Tuple2<String,Iterable<Tuple2<Integer, Integer>>> t:sortedCollections)
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
}
