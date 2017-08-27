package com.tryit.spark.accumulators;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;

/**
 * Created by agebriel on 2/10/17.
 */
public class MapAccumulator implements AccumulatorParam<Map<String, Long>>, Serializable
{
	@Override
	public Map<String, Long> addAccumulator(Map<String, Long> t1, Map<String, Long> t2)
	{
		return mergeMap(t1, t2);
	}

	@Override
	public Map<String, Long> addInPlace(Map<String, Long> r1, Map<String, Long> r2)
	{
		return mergeMap(r1, r2);
	}

	@Override
	public Map<String, Long> zero(Map<String, Long> initialValue)
	{
		return new HashMap<>();
	}

	private Map<String, Long> mergeMap( Map<String, Long> map1, Map<String, Long> map2) {
		Map<String, Long> result = new HashMap<>(map1);
		//map2.forEach((k, v) -> result.merge(k, v, (a, b) -> a + b));
		return result;
	}
}
