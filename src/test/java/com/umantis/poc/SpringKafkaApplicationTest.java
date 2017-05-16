package com.umantis.poc;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import com.umantis.poc.admin.KafkaAdminUtils;

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
    public KafkaAdminUtils kafkaAdminService;

    private static String TOPIC = "kafka-poc";

    @Value("${kafka.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }
    
    @Before
    public void setup() {
        if (kafkaAdminService.topicExists(TOPIC)) {
            TOPIC = TOPIC + System.currentTimeMillis();
        }
    }

    @Test
    public void testReceive() throws Exception {
        producer.send(TOPIC, "Hello kafka");

        consumer.latch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.latch().getCount()).isEqualTo(0);
    }
}
