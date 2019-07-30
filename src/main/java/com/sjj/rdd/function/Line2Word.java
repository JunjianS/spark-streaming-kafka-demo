package com.sjj.rdd.function;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.common.collect.Lists;

public class Line2Word implements FlatMapFunction<String, String> {

	private static final long serialVersionUID = 1L;

	private static final Pattern SPACE = Pattern.compile(" ");

	@Override
	public Iterable<String> call(String line) throws Exception {
		return Lists.newArrayList(SPACE.split(line));
	}

}
