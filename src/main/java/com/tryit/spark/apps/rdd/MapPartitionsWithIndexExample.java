package com.tryit.spark.apps.rdd;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   mapPartitionsWithIndex(func)
*   Similar to map Partitions, but also provides func with an integer value
*   representing the index of the partition, so func must be of type
*   (Int, Iterator<T>) ⇒ Iterator<U> when running on an RDD of type T.
*
*
*   ========================================================================
*
*   mapPartitions() can be used as an alternative to map() & foreach(). mapPartitions() is called
*   once for each Partition unlike map() & foreach() which is called for each element in the RDD.
*   The main advantage being that, we can do initialization on Per-Partition basis instead of
*   per-element basis(as done by map() & foreach())
*
*   Consider the case of Initializing a database. If we are using map() or foreach(), the number
*   of times we would need to initialize will be equal to the no of elements in RDD. Whereas if we
*   use mapPartitions(), the no of times we would need to initialize would be equal to number of
*   Partitions
*
*   We get Iterator as an argument for mapPartition, through which we can iterate through all the
*   elements in a Partition.
*
*   In this example, we will use mapPartitionsWithIndex(), which apart from similar to mapPartitions()
*   also provides an index to track the Partition No
*
*   ========================================================================
*/
public class MapPartitionsWithIndexExample
{
	public static void main(String[] args) throws Exception
	{
		//play with the parallelism and see how it behaves.
		JavaPairRDD<String, Integer> pairRDD = Util.getStrToIntPairRdd(3);

		JavaRDD<Integer> rddWithIndex
			= pairRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Integer>>()
		{
			@Override
			//This method is called ones per partition.
			public Iterator<Integer> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception
			{
				List<Integer> ls = new ArrayList<>();
				ls.add(v1); //collecting the indices
				System.out.println("Content of partion-" + v1);
				while(v2.hasNext())
				{
					Tuple2<String, Integer> t = v2.next();
					System.out.println(t._1()+" : "+t._2());
				}

				return ls.iterator();
			}
		}, false);

		System.out.println("Size of rddWithIndex is: " + rddWithIndex.count()); //returns number of partitions
		System.out.println("Size of pairRDD is: " + pairRDD.count()); //returns number of records
	}

}
