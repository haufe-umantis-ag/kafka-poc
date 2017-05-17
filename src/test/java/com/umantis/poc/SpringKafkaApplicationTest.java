package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.concurrent.TimeUnit;

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

    private static String TOPIC;

    @Value("${kafka.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Before
    public void setup() {
        if (!kafkaAdminService.topicExists(TOPIC)) {
            kafkaAdminService.createTopic(TOPIC, -1);
        }
    }

    @Test
    public void testReceive() throws Exception {

        BaseMessage message = BaseMessage.builder()
                .topic(TOPIC)
                .message("NO")
                .origin("SpringKafkaApplicationTest")
                .customerId("0")
                .build();
        producer.send(TOPIC, message);

        consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.getLatch().getCount()).isEqualTo(0);
    }
}
