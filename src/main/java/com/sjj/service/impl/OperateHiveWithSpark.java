package com.sjj.service.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Resource;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.collect.Sets;
import com.sjj.rdd.function.Accumulator;
import com.sjj.rdd.function.DStream2Row;
import com.sjj.rdd.function.Line2Word;
import com.sjj.rdd.function.MessageAndMeta;
import com.sjj.rdd.function.TupleValue;
import com.sjj.rdd.function.WordTick;
import com.sjj.service.IOperateHiveWithSpark;
import com.sjj.util.RedisUtil;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.streaming.Durations;

/**
 * spark streaming监听kafka消息，实现统计消息中单词出现次数，最后写入hive表
 * 
 * @author Tim
 *
 */
@Slf4j
@Service
public final class OperateHiveWithSpark implements IOperateHiveWithSpark {

	@Value("${kafka.broker}")
	private String kafkaBroker;

	@Value("${kafka.topic}")
	private String kafkaTopic;

	@Value("${spark.master}")
	private String sparkMaster;

	@Value("${spark.appName}")
	private String sparkAppName;

	@Value("${spark.ui.port}")
	private String sparkUiPort;

	@Value("${hadoop.user}")
	private String hadoopUser;

	@Value("${hive.db.name}")
	private String hiveDBName;

	@Resource
	RedisUtil redisUtil;

	// Hold a reference to the current offset ranges, so it can be used downstream
	final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

	public void launch() throws Exception {

		// kafka broker、topic参数校验
		if (StringUtils.isBlank(kafkaBroker) || StringUtils.isBlank(kafkaTopic)) {
			log.error("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		// 设置hadoop用户，本地环境运行需要，集群环境运行其实不需要
		if (StringUtils.isNotBlank(hadoopUser)) {
			System.setProperty("HADOOP_USER_NAME", hadoopUser);
		}

		// Spark应用的名称，可用于查看任务状态
		SparkConf sc = new SparkConf().setAppName(sparkAppName);

		// 配置spark UI的端口，默认是4040
		if (StringUtils.isNumeric(sparkUiPort)) {
			sc.set("spark.ui.port", sparkUiPort);
		}
		// 配置spark程序运行的环境，本地环境运行需要设置，集群环境在命令行以参数形式指定
		if (StringUtils.isNotBlank(sparkMaster)) {
			sc.setMaster(sparkMaster);
		}

		/*
		 * 创建上下文，60秒一个批次读取kafka消息，streaming的微批模式，这个时间的大小会影响写到hdfs或者hive中
		 * 文件个数，要根据kafka实际写入速度设置，避免生成太多小文件
		 */
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(60));

		// 组装kafka参数
		HashSet<String> topicsSet = Sets.newHashSet(StringUtils.split(kafkaTopic));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", kafkaBroker);

		// 获取消费kafka的offset
		// Hold a reference to the current offset ranges, so it can be used downstream
		Map<TopicAndPartition, Long> offsets = redisUtil.getOffset(topicsSet);

		JavaDStream<String> messages = null;

		// Create direct kafka stream with brokers and topics
		if (offsets.isEmpty()) {
			JavaPairInputDStream<String, String> pairDstream = KafkaUtils.createDirectStream(jssc, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
			messages = pairDstream
					.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
						private static final long serialVersionUID = 1L;

						public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
							OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
							offsetRanges.set(offsets);
							return rdd;
						}
					}).flatMap(new TupleValue());
		} else {
			JavaInputDStream<String> dStream = KafkaUtils.createDirectStream(jssc, String.class, String.class,
					StringDecoder.class, StringDecoder.class, String.class, kafkaParams, offsets, new MessageAndMeta());

			messages = dStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
				private static final long serialVersionUID = 1L;

				public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
					OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
					offsetRanges.set(offsets);
					return rdd;
				}
			});
		}

		JavaDStream<String> words = messages.flatMap(new Line2Word());

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new WordTick()).reduceByKey(new Accumulator());

		HiveContext hiveContext = new HiveContext(jssc.sparkContext());
		List<String> schemaString = new ArrayList<String>();
		schemaString.add("word");
		schemaString.add("count");

		StructType schema = new StructType(
				new StructField[] { new StructField("word", DataTypes.StringType, false, Metadata.empty()),
						new StructField("count", DataTypes.IntegerType, false, Metadata.empty()) });

		JavaDStream<Row> rowDStream = wordCounts.flatMap(new DStream2Row());

		rowDStream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Row> rowRDD) throws Exception {

				if (!rowRDD.isEmpty()) {
					log.error(">>>" + rowRDD.partitions().size());

					hiveContext.sql("set hive.exec.stagingdir = /tmp/staging/.hive-staging");
					hiveContext.sql("use " + hiveDBName);

					DataFrame df = hiveContext.createDataFrame(rowRDD, schema).coalesce(10);

					df.registerTempTable("wc");
					hiveContext.sql("insert into test_wc select word,count from wc");

					// 更新offset
					for (OffsetRange offsetRange : offsetRanges.get()) {
						redisUtil.setOffset(offsetRange);
					}

				}
			}
		});

		wordCounts.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
