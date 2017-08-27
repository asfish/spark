package com.tryit.spark.apps.rdd;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
*   When called on a dataset of (K, V) pairs, returns a dataset
*   of (K, U) pairs where the values for each key are aggregated
*   using the given combine functions and a neutral "zero" value.
*   Allows an aggregated value type that is different from the
*   input value type, while avoiding unnecessary allocations.
*   Like in groupByKey, the number of reduce tasks is configurable
*   through an optional second argument.
*
*   aggregateByKey() is almost identical to reduceByKey()
*   (both calling combineByKey() behind the scenes), except
*   you give a starting value for aggregateByKey(). Most people
*   are familiar with reduceByKey(), so I will use that in the explanation.
*
*   The aggregateByKey function requires 3 parameters:
*
* 1. An intitial ‘zero’ value that will not effect the total values to be collected.
* For example if we were adding numbers the initial value would be 0. Or in the case of
* collecting unique elements per key, the initial value would be an empty set.
*
* 2. A combining function accepting two paremeters. The second paramter is merged into the
* first parameter. This function combines/merges values within a partition.
*
* 3. A merging function, function accepting two parameters. In this case the paremters are
* merged into one. This step merges values across partitions.

*/
public class AggregateByKeyExample
{
	public static void main(String[] args) throws Exception
	{
		JavaSparkContext sc = new JavaSparkContext("local", "AggregateByKeyExample");
		JavaRDD<String>  base = sc
			.parallelize(Arrays.asList("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D"));

		JavaPairRDD<String, String> pairRdd = base.mapToPair(new PairFunction<String, String, String>()
		{
			@Override
			public Tuple2<String, String> call(String s) throws Exception
			{
				String[] split = s.split("=");
				return new Tuple2<>(split[0], split[1]);
			}
		});

		Set<String> initVal = new HashSet<String>();

		JavaPairRDD<String, Set<String>> aggregatedRdd = pairRdd.aggregateByKey(initVal,
			new Function2<Set<String>, String, Set<String>>()
			{
				@Override
				public Set<String> call(Set<String> v1, String v2) throws Exception
				{
					v1.add(v2);
					return v1;
				}
			},
			new Function2<Set<String>, Set<String>, Set<String>>()
			{
				@Override
				public Set<String> call(Set<String> v1, Set<String> v2) throws Exception
				{
					v1.addAll(v2);
					return v1;
				}
			});

		aggregatedRdd.foreach(new VoidFunction<Tuple2<String, Set<String>>>()
		{
			@Override
			public void call(Tuple2<String, Set<String>> t2) throws Exception
			{
				System.out.println(t2._1() + " -> " + t2._2());
			}});

		System.out.println("--------------------------------------------------");

		int initSume = 0;
		JavaPairRDD<String, Integer> aggSum = pairRdd.aggregateByKey(initSume,
			new Function2<Integer, String, Integer>()
			{
				@Override
				public Integer call(Integer v1, String v2) throws Exception
				{
					return v1+=1;
				}
			},
			new Function2<Integer, Integer, Integer>()
			{
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception
			{
				return v1+v1;
			}}) ;

		aggSum.foreach(new VoidFunction<Tuple2<String, Integer>>()
		{
			@Override
			public void call(Tuple2<String, Integer> t2) throws Exception
			{
				System.out.println(t2._1() + " -> " + t2._2());
			}
		});

		//sc.close();
		sc.stop();

		}
}
