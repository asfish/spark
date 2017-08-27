package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Iterables;

import scala.Tuple2;

/*
*   cogroup(otherDataset, [numTasks])
*   When called on datasets of type (K, V) and (K, W), returns a dataset
*   of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also
*   called group With.
 */
public class CoGroupExample
{
	public static void main(String[] args) throws Exception
	{
		JavaSparkContext sc = new JavaSparkContext(
			"local", "IntersectByKey", System.getenv("SPARK_HOME"), System.getenv("JARS"));

		List<Tuple2<String, Integer>> input1 = new ArrayList();
		input1.add(new Tuple2("coffee", 1));
		input1.add(new Tuple2("coffee", 2));
		input1.add(new Tuple2("pandas", 3));
		input1.add(new Tuple2("elephant", 5));
		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(input1);

		List<Tuple2<String, Integer>> input2 = new ArrayList();
		input2.add(new Tuple2("pandas", 20));
		input2.add(new Tuple2("pandas", 30));
		input2.add(new Tuple2("lion", 50));
		JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(input2);

		JavaPairRDD<String, Integer> result = intersectByKey(rdd1, rdd2);
		for (Tuple2<String, Integer> entry : result.collect())
		{
			System.out.println(entry._1() + ":" + entry._2());
		}
		System.out.println("Done");
		sc.close();
	}

	public static <K, V> JavaPairRDD<K, V> intersectByKey(JavaPairRDD<K, V> rdd1, JavaPairRDD<K, V> rdd2)
	{
		JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<V>>> grouped = rdd1.cogroup(rdd2);
		System.out.println("Co-grouped rdd execution plan: \n" + grouped.toDebugString());

		for (Tuple2<K, Tuple2<Iterable<V>, Iterable<V>>> entry : grouped.collect())
		{
			System.out.println(entry._1() + "=>" + entry._2());
		}

		return grouped.flatMapValues(new Function<Tuple2<Iterable<V>, Iterable<V>>, Iterable<V>>() {
			@Override
			public Iterable<V> call(Tuple2<Iterable<V>, Iterable<V>> input) {
				ArrayList<V> al = new ArrayList<V>();

				if (!Iterables.isEmpty(input._1()) && !Iterables.isEmpty(input._2())) {
					Iterables.addAll(al, input._1());
					Iterables.addAll(al, input._2());
				}
				return al;
			}
		});
	}
}
