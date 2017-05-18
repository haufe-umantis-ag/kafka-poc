package com.umantis.poc.config;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.umantis.poc.admin.KafkaAdminUtils;

@Configuration
public class CommonConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CommonConfig.class);

	@Autowired
	public KafkaAdminUtils kafkaAdminService;

	@Bean("kafkaTopicRandom")
	public String kafkaTopicRandom(@Value("${kafka.topic}") String topic) {
		String kafkaTopicRandom = topic + "." + RandomStringUtils.randomAlphabetic(8);
		LOGGER.debug("Random topic name is {}", kafkaTopicRandom);
		if (!kafkaAdminService.topicExists(kafkaTopicRandom)) {
			kafkaAdminService.createTopic(kafkaTopicRandom, -1);
			LOGGER.info("Created topic {}", kafkaTopicRandom);
		}
		return kafkaTopicRandom;
	}

}
