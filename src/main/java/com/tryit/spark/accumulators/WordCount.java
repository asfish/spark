package com.tryit.spark.accumulators;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * Created by agebriel on 2/10/17.
 */
public class WordCount
{
	static final String INPUT_FILE_TEXT = "/Users/agebriel/Desktop/notes/spark-conf.txt";
	static final String OUTPUT_FILE_TEXT = "";


	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local")
			.setAppName("Word Count with Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(INPUT_FILE_TEXT);

		//final Accumulator<Integer> blankLines = sc.accumulator(0);
		final LongAccumulator blankLines = sc.sc().longAccumulator("sum");

		@SuppressWarnings("resource")
		JavaPairRDD<String, Integer> counts = lines.flatMap(new FlatMapFunction<String, String>()
		{
			@Override
			public Iterator<String> call(String s) throws Exception
			{
				if ("".equals(s)) {
					blankLines.add(1);
				}
				return Arrays.asList(s.split(" ")).iterator();
			}
		})
		.mapToPair(new PairFunction<String, String, Integer>()
		{
			@Override
			public Tuple2<String, Integer> call(String str) throws Exception
			{
				return new Tuple2<String, Integer>(str, 1);
			}
		})

		.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception
			{
				return v1 + v2;
			}
		});

		//counts.saveAsTextFile(OUTPUT_FILE_TEXT);
		counts.foreach(new VoidFunction<Tuple2<String, Integer>>()
		{
			@Override
			public void call(Tuple2<String, Integer> tuple2) throws Exception
			{
				System.out.println(tuple2._1()+ " => " + tuple2._2());
			}
		});



		System.out.println("Blank lines: " + blankLines.value());

		sc.close();
	}
}
