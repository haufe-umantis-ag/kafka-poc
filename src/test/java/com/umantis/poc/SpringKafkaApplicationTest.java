package com.umantis.poc;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.umantis.poc.model.BaseMessage;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaApplicationTest {

    @Autowired
    public Producer producer;

    @Autowired
    public Consumer consumer;

	@Autowired
	@Qualifier("kafkaTopicRandom")
	String topic;

    @Test
	public void testReceive() throws Exception {

        BaseMessage message = BaseMessage.builder()
				.topic(topic)
                .message("NO")
                .origin("SpringKafkaApplicationTest")
                .customerId("0")
                .build();
		producer.send(topic, message);

        consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.getLatch().getCount()).isEqualTo(0);
    }
}
