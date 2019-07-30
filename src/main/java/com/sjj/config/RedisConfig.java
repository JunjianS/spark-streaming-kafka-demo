package com.sjj.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * redis客户端Lettuce配置,springboot会根据配置文件自动创建LettuceConnectionFactory
 * springboot2.0默认使用Lettuce客户端，用jedis包替换掉Lettuce包，在application.yml的Redis标签下添加jedis配置，即可实现jedis客户端的切换
 * 
 */
@Slf4j
@Configuration
public class RedisConfig{

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate template = new RedisTemplate();
        template.setConnectionFactory(factory);

        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(Object.class);
        jackson2JsonRedisSerializer.setObjectMapper(new ObjectMapper().setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY).enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL));
        template.setValueSerializer(jackson2JsonRedisSerializer);
        
        //解决key出现乱码前缀\xac\xed\x00\x05t\x00\tb，这个问题实际并不影响使用，只是去Redis里查找key的时候会出现疑惑
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringRedisSerializer);
        template.afterPropertiesSet();
        log.info(">>> Lettuce客户端创建成功");
        return template;
    }
}
