package com.sjj.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.spark.streaming.kafka.OffsetRange;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.sjj.service.impl.OperateHiveWithSpark;

import kafka.common.TopicAndPartition;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedisUtil {

	private static final String KAFKA_PATITION_REDIS_KEY_SUFFIX = ".partition";
	private static final String KAFKA_OFFSET_REDIS_KEY_SUFFIX = ".offset";

	@Resource
	RedisTemplate<String, String> redisTemplate;

	public Map<TopicAndPartition, Long> getOffset(Set<String> topics) {
		return getOffsetFromRedis(topics);
	}

	public void setOffset(OffsetRange offsetRange) {
		setOffsetToRedis(offsetRange);
	}

	private Map<TopicAndPartition, Long> getOffsetFromRedis(Set<String> topics) {
		Map<TopicAndPartition, Long> offsets = new HashMap<TopicAndPartition, Long>();

		for (String topic : topics) {
			try {
				Integer partation = Integer
						.parseInt(redisTemplate.opsForValue().get(topic + KAFKA_PATITION_REDIS_KEY_SUFFIX));
				Long offset = Long.parseLong(redisTemplate.opsForValue().get(topic + KAFKA_OFFSET_REDIS_KEY_SUFFIX));
				if (null != partation && null != offset) {
					log.info("### get kafka offset in redis for kafka ### " + topic + KAFKA_OFFSET_REDIS_KEY_SUFFIX
							+ " >>> " + partation + " | " + offset);
					offsets.put(new TopicAndPartition(topic, partation), offset);
				}
			} catch (NumberFormatException e) {
				log.error("### Topic: " + topic + " offset exception ###");
			}
		}
		return offsets;
	}

	private void setOffsetToRedis(OffsetRange offsetRange) {
		redisTemplate.opsForValue().set(offsetRange.topic() + KAFKA_PATITION_REDIS_KEY_SUFFIX,
				String.valueOf(offsetRange.partition()));
		redisTemplate.opsForValue().set(offsetRange.topic() + KAFKA_OFFSET_REDIS_KEY_SUFFIX,
				String.valueOf(offsetRange.fromOffset()));

		log.info("### update kafka offset in redis ### " + offsetRange.topic() + KAFKA_PATITION_REDIS_KEY_SUFFIX
				+ " >>> " + offsetRange);

	}
}
