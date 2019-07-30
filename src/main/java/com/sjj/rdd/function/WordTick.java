package com.sjj.rdd.function;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordTick implements PairFunction<String, String, Integer> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Integer> call(String s) {
		return new Tuple2<String, Integer>(s, 1);
	}
}
