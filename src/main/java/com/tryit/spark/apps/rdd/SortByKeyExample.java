package com.tryit.spark.apps.rdd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import javax.persistence.Tuple;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   sortByKey([ascending], [numTasks])
*   When called on a dataset of (K, V) pairs where K implements Ordered,
*   returns a dataset of (K, V) pairs sorted by keys in ascending or
*   descending order, as specified in the Boolean ascending argument.
 */
public class SortByKeyExample
{
	public static void main(String[] args) throws Exception
	{
		List<Tuple2<String, Integer>> input = new ArrayList();
		input.add(new Tuple2("coffee2", 2));
		input.add(new Tuple2("coffee9", 9));
		input.add(new Tuple2("pandas3", 3));
		input.add(new Tuple2("pandas4", 4));
		input.add(new Tuple2("pandas6", 6));
		input.add(new Tuple2("coffee7", 7));
		input.add(new Tuple2("pandas11", 11));
		input.add(new Tuple2("pandas2", 2));
		input.add(new Tuple2("coffee1", 1));
		input.add(new Tuple2("pandas4", 4));

		//play with the parallelism and see how it behaves.
		JavaPairRDD<String, Integer> pairRDD = Util.sc().parallelizePairs(input, 3);
		pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>()
		{
			@Override
			public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception
			{
				System.out.println(stringIntegerTuple2._1()+" => "+ stringIntegerTuple2._2());
			}
		});

		//sortByKey() results in range-partitioned RDDs
		JavaPairRDD<String, Integer> sortRdd = pairRDD.sortByKey();
		sortRdd.foreach(new VoidFunction<Tuple2<String, Integer>>()
		{
			@Override
			public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception
			{
				System.out.println(stringIntegerTuple2._1()+" => "+ stringIntegerTuple2._2());
			}
		});

		Map<String, Integer> sortMap = sortRdd.collectAsMap(); //you'll lose the order b/c Map does not grantee it.
		for(Map.Entry e : sortMap.entrySet())
			System.out.print(e.getKey() +" => "+ e.getValue() + "\n");

		List<Tuple2<String, Integer>> tuples = sortRdd.collect(); //you'll lose the order b/c Map does not grantee it.
		for(Tuple2<String, Integer> t : tuples)
			System.out.print(t._1() +" => "+ t._2() + "\n");
	}
}
