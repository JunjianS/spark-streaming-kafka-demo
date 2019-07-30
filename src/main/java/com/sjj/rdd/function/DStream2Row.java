package com.sjj.rdd.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

public class DStream2Row implements FlatMapFunction<Tuple2<String, Integer>, Row>{
	private static final long serialVersionUID = 5481855142090322683L;

	@Override
	public Iterable<Row> call(Tuple2<String, Integer> t) throws Exception {
		List<Row> list = new ArrayList<>();
		list.add(RowFactory.create(t._1, t._2));

		return list;
	}
}
