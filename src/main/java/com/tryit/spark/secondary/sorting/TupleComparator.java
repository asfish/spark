package com.tryit.spark.secondary.sorting;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

/**
 * Created by agebriel on 12/28/16.
 */
public class TupleComparator implements Comparator<Tuple2<Integer, String>>, Serializable
{
	@Override
	public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
		return tuple1._1 < tuple2._1 ? 0 : 1;
	}
}
