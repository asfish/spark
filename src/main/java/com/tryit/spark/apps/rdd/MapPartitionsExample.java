package com.tryit.spark.apps.rdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.runtime.Resources;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;

import com.cloudera.livy.shaded.kryo.kryo.util.ObjectMap;
import com.google.common.collect.Iterables;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import jodd.util.CollectionUtil;
import scala.Product2;
import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   mapPartitions(func)
*   Similar to map, but runs separately on each partition (block) of
*   the RDD, so func must be of type Iterator<T> ⇒ Iterator<U> when
*   running on an RDD of type T.
 */
public class MapPartitionsExample
{
	public static void main(String[] args) throws Exception
	{
		List<Tuple2<String, Integer>> input = new ArrayList();
		input.add(new Tuple2<>("maths", 50));
		input.add(new Tuple2<>("maths", 60));
		input.add(new Tuple2<>("english", 65));
		input.add(new Tuple2<>("physics", 66));
		input.add(new Tuple2<>("physics", 61));
		input.add(new Tuple2<>("english", 75));
		input.add(new Tuple2<>("physics", 87));

		JavaRDD<Tuple2<String, Integer>> pairRDD = Util.sc().parallelize(input, 2);

		JavaRDD<Tuple2<String, Iterable<Integer>>> partitionRDD=pairRDD.mapPartitions(
			new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Iterable<Integer>>>()
			{
				@Override
				public Iterator<Tuple2<String, Iterable<Integer>>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator)
					throws Exception
				{
					//Map<String, List<Integer>> mp = new HashMap<>();
					ObjectMap<String, List<Integer>> mp = new ObjectMap<>();
					while(tuple2Iterator.hasNext())
					{
						Tuple2<String, Integer> tuple2 = tuple2Iterator.next();
						if(mp.containsKey(tuple2._1()))
						{
							mp.get(tuple2._1()).add(tuple2._2());
						}
						else
						{
							List<Integer> ls = new ArrayList<>();
							ls.add(tuple2._2());
							mp.put(tuple2._1(), ls);
						}
					}
					//List<Tuple2<String, Iterable<Integer>>> tps = new ArrayList<>();
					ObjectList<Tuple2<String, Iterable<Integer>>> tps = new ObjectArrayList<>();
					for(String key : mp.keys())
					{
						System.out.println(key + " : " + mp.get(key));
						tps.add(new Tuple2<>(key, (Iterable<Integer>)mp.get(key)));
					}

					return tps.iterator();
				}
			});

		System.out.println("Size of rddWithIndex is: " + partitionRDD.count()); //returns number of partitions
		System.out.println("Size of pairRDD is: " + pairRDD.count()); //returns number of records

		//pair rdd them
		JavaPairRDD<String, Iterable<Integer>> pRDD = partitionRDD.mapToPair(
			new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>()
			{
				@Override
				public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple2)
					throws Exception
				{
					return new Tuple2<>(tuple2._1(), tuple2._2());
				}
			});

		Map<String, Iterable<Integer>> map = pRDD.collectAsMap();
		for(String str : map.keySet())
			System.out.println(str +" => "+map.get(str));


		// Decrease the number of partitions in the RDD to numPartitions. Useful for
		// running operations more efficiently after filtering down a large dataset.
		JavaPairRDD<String, Iterable<Integer>> coalesceRDD = pRDD.coalesce(1, true);
		//List<Tuple2<String,Iterable<Integer>>> ct = coalesceRDD.collect();
		System.out.println("RDD partition size after coalesce operator: " + coalesceRDD.partitions().size());

		//reduce by key
		JavaPairRDD<String, Iterable<Integer>> reducedRDD = coalesceRDD.reduceByKey(
			new Function2<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>()
			{
				//List<Integer> ls = new ArrayList<>();
				IntArrayList ls = new IntArrayList();
				@Override
				public Iterable<Integer> call(Iterable<Integer> v1, Iterable<Integer> v2) throws Exception
				{
					ls.add(v1.iterator().next());
					ls.add(v2.iterator().next());
					return ls;
				}
			});

		ObjectList<Tuple2<String, Iterable<Integer>>> list = new ObjectArrayList<>(reducedRDD.collect());
		//List<Tuple2<String, Iterable<Integer>>> list = reducedRDD.collect();
		for(Tuple2<String, Iterable<Integer>> t:list)
			System.out.println(t._1()+" : "+t._2());

	}

}
