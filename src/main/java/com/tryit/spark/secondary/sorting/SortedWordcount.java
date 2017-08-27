package com.tryit.spark.secondary.sorting;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.antlr.v4.runtime.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Function2;
import scala.Tuple2;

/**
 * Created by agebriel on 12/29/16.
 */
public class SortedWordcount
{
	/*
	//A use case would be sortedWordcount("I fish a fish") ->  [(fish, 2), (I,1), (a,1))]
	public List<String> sortedWordcount() {
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("myFile.txt");

		// classical wordcount, nothing new there
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		// to enable sorting by value (count) and not key -> value-to-key conversion pattern
		JavaPairRDD<Tuple2<Integer, String>, Integer> countInKey = counts.mapToPair(a -> new Tuple2(new Tuple2<Integer, String>(a._2, a._1), null)); // setting value to null, since it won't be used anymore

		List<Tuple2<Tuple2<Integer, String>, Integer>> wordSortedByCount = countInKey.sortByKey(new TupleComparator(), false).collect();

		List<String> result = new ArrayList<>();
		IntStream.range(0, wantedSize).forEach(i -> result.add(wordSortedByCount.get(i)._1._2));
		return result;
	}

	private class TupleComparator implements Comparator<Tuple2<Integer, String>>, Serializable
	{
		@Override
		public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
			return tuple1._1 < tuple2._1 ? 0 : 1;
		}
	}
*/
}
