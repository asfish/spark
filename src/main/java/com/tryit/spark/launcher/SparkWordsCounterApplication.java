package com.tryit.spark.launcher;

/**
 * Created by agebriel on 6/27/17.
 */
public class SparkWordsCounterApplication
{
	public static void main(String[] args) {
		SparkService srv = new SparkService();

		srv.countWords("book.txt");

		srv.getJobResults();
	}
}
