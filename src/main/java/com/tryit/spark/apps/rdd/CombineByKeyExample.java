package com.tryit.spark.apps.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


/**
 * Created by agebriel on 12/13/16.
 */

/*
*   combineByKey() combines values with the same key, but uses a different
*   result type.
*/
public class CombineByKeyExample
{
	public static void main(String[] args) throws Exception
	{
		List<Tuple2<String, Integer>> input = new ArrayList();
		input.add(new Tuple2("coffee_2", 1));
		input.add(new Tuple2("coffee_1", 2));
		input.add(new Tuple2("coffee_1", 7));
		input.add(new Tuple2("coffee_2", 9));
		input.add(new Tuple2("pandas_2", 3));
		input.add(new Tuple2("pandas_1", 4));
		input.add(new Tuple2("pandas_3", 6));
		input.add(new Tuple2("pandas_2", 11));
		input.add(new Tuple2("pandas_3", 2));
		input.add(new Tuple2("pandas_1", 5));

		//play with the parallelism and see how it behaves.
        JavaPairRDD<String, Integer> pairRDD = Util.sc().parallelizePairs(input,5);

		/*
		 * This is called when a key(in the RDD element) is found for the first
		 * time in a given Partition. This method creates an initial value
		 * for the accumulator for that key
    	*/
		Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
			@Override
			public AvgCount call(Integer x) {
				System.out.println("createAcc, with unique key: " + x);
				return new AvgCount(x, 1);
			}
		};

		//This is called when the key already has an accumulator
		Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
			@Override
			public AvgCount call(AvgCount a, Integer x) {
				System.out.println("addAndCount: "+x);
				a.total_ += x;
				a.num_ += 1;
				return a;
			}
		};

		//This is called when more that one partition has accumulator for the same key
		Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
			@Override
			public AvgCount call(AvgCount a, AvgCount b) {
				System.out.println("combine");
				a.total_ += b.total_;
				a.num_ += b.num_;
				return a;
			}
		};
		
		JavaPairRDD<String, AvgCount> avgCounts = pairRDD.combineByKey(createAcc, addAndCount, combine);

		Map<String, AvgCount> countMap = avgCounts.collectAsMap();

		for (Entry<String, AvgCount> entry : countMap.entrySet())
		{
			System.out.println("Key:" + entry.getKey());
			System.out.println("Count: " + entry.getValue().num_);
			System.out.println("sum: " + entry.getValue().total_);
			System.out.println("Average: " + entry.getValue().avg());
		}
	}

	public static class AvgCount implements java.io.Serializable {
		public AvgCount(int total, int num) {
			total_ = total;
			num_ = num;
		}
		public int total_;
		public int num_;
		public float avg() {
			return total_ / (float) num_;
		}
	}
}
