package com.tryit.spark.apps.rdd;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */
public class Util
{
	public static JavaSparkContext sc()
	{
		SparkConf conf = new SparkConf()
			.setAppName("ReduceByKeyExample")
			.setMaster("local")

			//.set("spark.yarn.dist.files","/Users/agebriel/fox/foxservices/SparkService/spark-service/livy/metrics.properties")
			//.set("spark.metrics.conf","/Users/agebriel/fox/foxservices/SparkService/spark-service/livy/metrics.properties")
			/*.set("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
			.set("spark.metrics.conf.*.sink.graphite.host", "192.168.1.7")
			.set("spark.metrics.conf.*.sink.graphite.port", "2003")
			.set("spark.metrics.conf.*.sink.graphite.period", "10")
			.set("spark.metrics.conf.*.sink.graphite.unit","seconds")
			.set("spark.metrics.conf.*.sink.graphiteprefix", "cf_spark_")

			.set("spark.metrics.conf.master.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
			.set("spark.metrics.conf.worker.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
			.set("spark.metrics.conf.driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
			.set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")*/
			;

		JavaSparkContext sc = new JavaSparkContext(conf);

		return sc;
	}

	public static JavaPairRDD<String, Integer> getStrToIntPairRdd(int partitions)
	{
		List<Tuple2<String, Integer>> input = new ArrayList();
		input.add(new Tuple2("coffee", 2));
		input.add(new Tuple2("coffee", 9));
		input.add(new Tuple2("pandas", 3));
		input.add(new Tuple2("pandas", 4));
		input.add(new Tuple2("pandas", 6));
		input.add(new Tuple2("coffee", 7));
		input.add(new Tuple2("pandas", 11));
		input.add(new Tuple2("pandas", 2));
		input.add(new Tuple2("coffee", 1));
		input.add(new Tuple2("pandas", 4));

		//play with the parallelism and see how it behaves.
		JavaPairRDD<String, Integer> pairRDD = Util.sc().parallelizePairs(input, 3);

		return pairRDD;
	}


	public static void regAccumulator(JavaSparkContext sc)
	{
		// Create an accumulator to count how many rows might be inaccurate.
		LongAccumulator la = sc.sc().longAccumulator("Height");

		// Create an accumulator to store all questionable values.
		//StringAccumulator heightValues = new StringAccumulator(
		//	0, null, "Height", false);

		//sc.sc().register(heightValues);
	}
}
