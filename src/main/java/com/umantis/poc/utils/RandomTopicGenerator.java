package com.umantis.poc.utils;

import com.umantis.poc.admin.KafkaAdminUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RandomTopicGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RandomTopicGenerator.class);

	@Autowired
	public KafkaAdminUtils kafkaAdminService;

	@Bean("kafkaTopicRandom")
	public String kafkaTopicRandom(@Value("${kafka.topic}") String topic) {
		String kafkaTopicRandom = topic + "." + RandomStringUtils.randomAlphabetic(8);
		LOGGER.info("Random topic name is {}", kafkaTopicRandom);
		if (!kafkaAdminService.topicExists(kafkaTopicRandom)) {
			kafkaAdminService.createTopic(kafkaTopicRandom, -1);
			LOGGER.info("Created topic {}", kafkaTopicRandom);
		}
		return kafkaTopicRandom;
	}

	@Bean("kafkaTopicRandomGeneric")
	public String kafkaTopicRandomGeneric(@Value("${kafka.topic}") String topic) {
		String kafkaTopicRandom = topic + "." + RandomStringUtils.randomAlphabetic(8);
		LOGGER.info("Random topic name for Generic messages is {}", kafkaTopicRandom);
		if (!kafkaAdminService.topicExists(kafkaTopicRandom)) {
			kafkaAdminService.createTopic(kafkaTopicRandom, -1);
			LOGGER.info("Created topic {}", kafkaTopicRandom);
		}
		return kafkaTopicRandom;
	}

}
