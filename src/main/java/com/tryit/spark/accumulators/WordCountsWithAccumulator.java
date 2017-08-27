package com.tryit.spark.accumulators;

import java.io.FileNotFoundException;
import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 * Created by agebriel on 2/19/17.
 */
public class WordCountsWithAccumulator
{
    static final String INPUT_FILE_TEXT = "/Users/agebriel/Desktop/notes/spark-conf.txt";

    public static void main(String[] args)
        throws FileNotFoundException
    {
        SparkConf conf = new SparkConf()
	        .setMaster("local")
	        .setAppName("Third App - Word Count WITH BroadCast and Accumulator");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = jsc.textFile(INPUT_FILE_TEXT);

        JavaRDD<String> words = fileRDD.flatMap(new FlatMapFunction<String, String>()
        {
            public java.util.Iterator<String> call(String aLine)
                throws Exception
            {
                return Arrays.asList(aLine.split(" ")).iterator();
            }
        }).repartition(4);
        String[] stopWordArray = new String[] {"when", "spark", };
        final Accumulator<Integer> skipAccumulator = jsc.accumulator(0);
        final Accumulator<Integer> unSkipAccumulator = jsc.accumulator(0);
        final Broadcast<String[]> stopWordBroadCast = jsc.broadcast(stopWordArray);

        JavaRDD<String> filteredWords = words.filter(new Function<String, Boolean>()
        {
            public Boolean call(String inString)
                throws Exception
            {
                boolean filterCondition = !Arrays.asList(stopWordBroadCast.getValue()).contains(inString);
                if (!filterCondition)
                {
                    //System.out.println("Filtered a stop word ");
                    skipAccumulator.add(1);
                }
                else
                {
                    unSkipAccumulator.add(1);
                }
                return filterCondition;
            }
        });
	    System.out.println("Num of cores: " + filteredWords.partitions().size());
	    System.out.println(filteredWords.top(2));

        System.out.println("$$$$$$$$Filtered Count " + skipAccumulator.value());
        System.out.println("$$$$$$$$ UN Filtered Count " + unSkipAccumulator.value());

	    //System.out.println(filteredWords.top(2));
        /* rest of code - works fine */
        jsc.stop();
        jsc.close();
    }
}
