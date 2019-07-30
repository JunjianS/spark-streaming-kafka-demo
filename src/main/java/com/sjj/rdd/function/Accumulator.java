package com.sjj.rdd.function;

import org.apache.spark.api.java.function.Function2;

public class Accumulator implements Function2<Integer, Integer, Integer> {
	private static final long serialVersionUID = 1L;

	@Override
	public Integer call(Integer i1, Integer i2) {
		return i1 + i2;
	}
}
