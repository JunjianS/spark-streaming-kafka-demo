package com.sjj.rdd.function;

import org.apache.spark.api.java.function.Function;
import kafka.message.MessageAndMetadata;

public class MessageAndMeta implements Function<MessageAndMetadata<String, String>, String> {

	private static final long serialVersionUID = 1L;

	public String call(MessageAndMetadata<String, String> messageAndMetadata) throws Exception {
		return messageAndMetadata.message();
	}
}
