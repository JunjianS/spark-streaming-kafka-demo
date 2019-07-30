package com.sjj.rdd.function;

import java.util.Arrays;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class TupleValue implements FlatMapFunction<Tuple2<String, String>,String> {
	private static final long serialVersionUID = 1L;

	@Override
	public Iterable<String> call(Tuple2<String, String> t) throws Exception {
		return Arrays.asList(t._2);
	}
}
