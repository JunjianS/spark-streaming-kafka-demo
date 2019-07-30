# spark-streaming-kafka-demo
**使用Springboot框架，Sparkstreaming监听Kafka消息，Redis记录已读Kafka偏移量，Spark统计单词出现次数，最后写入Hive表。**


### 注意事项

1. **版本信息**
- Kafka：2.12-2.3.0
- Spark：1.6.0
- Redis：4.x
- Hadoop：2.6.0-cdh5.15.2
- 
2. **读取Kafka** 

   采用Direct方式：此方式不使用接收器接收数据，而是周期性查询Kafka中每个主题+分区中的最新偏移量，并相应地定义要在每批中处理的偏移量范围。
 

```
   JavaInputDStream<String> dStream = KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    String.class,
					          StringDecoder.class,
					          StringDecoder.class,
					          String.class,
					          kafkaParams,
					          offsets,
					          new MessageAndMeta());
```

   正常处理RDD后，更新Offset到Redis
```
   // 更新offset
   for (OffsetRange offsetRange : offsetRanges.get()) {
		setOffsetToRedis(offsetRange);
   }
```


   每次启动前从Redis获取上次读取的Offset
```
   // 获取消费kafka的offset
   getOffset(topicsSet);
```

   
   处理数据的作业启动后，Kafka consumerAPI读取Kafka中定义的偏移量范围（类似于从文件系统读取文件），每60s读取一次Kafka消息，这个间隔时间建议根据kafka写入速度自行设置，Sparkstreaming的微批处理模式，Spark处理后每批写一个数据文件（直接写hdfs或者hive表），如果每批读取的文件太少，会造成大量小文件（大量小文件的问题请自行bing）。当消息为空时，Spark任务会生成空文件，为避免生成空文件，在操作前对RDD进行非空判断
   
   
   
```
  if (!rowRDD.isEmpty()) {
  //操作RDD
  }
```

   
   
3. **RDD操作**

   示例是计算单词频次，Spark的transformation算子用到的function需要序列化，所以如果使用匿名类，那匿名类所在的宿主类也必须能序列化，所以示例中把flatMap、reduceByKey等算子的function单独定义了类，放在util目录下。
   
   spark任务默认会在Hive表数据文件目录生成staging文件，可以配置到统一目录定时清理
   
   
```
  hiveContext.sql("set hive.exec.stagingdir = /tmp/staging/.hive-staging")
```

   
4. 运行
   - 本地运行
     按照Springboot的方式直接运行主类 SprakStreamingMain
   - 集群环境运行
     - 打包
       
       Spark不支持使用spring-boot-maven-plugin打包的springboot项目结构，所以本项目使用maven-shade-plugin插件打包成一个fat的jar包；因为集群中
       一般都有相关的jar包，所有Spark相关的jar包都不需要打进jar包，在pom中把scope设置成provided。

     - 运行
     
       有多种运行模式，

Master参数  | 含义
---|---
local | 使用1个worker线程在本地运行Spark应用程序
local[K] | 使用K个worker线程在本地运行Spark应用程序
local. | 使用所有剩余worker线程在本地运行Spark应用程序
spark://HOST:PORT | 连接到Spark Standalone集群，以便在该集群上运行Spark应用程序
mesos://HOST:PORT	 | 连接到Mesos集群，以便在该集群上运行Spark应用程序
yarn-client	 | 以client方式连接到YARN集群，集群的定位由环境变量HADOOP_CONF_DIR定义，该方式driver在client运行。
yarn-cluster	 | 以cluster方式连接到YARN集群，集群的定位由环境变量HADOOP_CONF_DIR定义，该方式driver也在集群中运行。

举个使用yarn集群的例子，通过参数properties-file指定springboot配置文件
```
spark-submit --master yarn-cluster  --num-executors 2 --driver-memory 128m --executor-memory 128m --executor-cores 2  --class com.sjj.SprakStreamingMain --properties-file /root/application.properties spark-demo-boot.jar
```




