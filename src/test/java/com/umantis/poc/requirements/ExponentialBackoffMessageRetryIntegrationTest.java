package com.umantis.poc.requirements;

import static org.assertj.core.api.Assertions.assertThat;

import com.umantis.poc.Consumer;
import com.umantis.poc.Producer;
import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;
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
public class ExponentialBackoffMessageRetryIntegrationTest {

    @Autowired
    public KafkaAdminUtils kafkaAdminService;

    @Autowired
    public Producer producer;

    @Autowired
    public Consumer consumer;

    private static String TOPIC;

    @Value("${kafka.seeker_topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Before
    public void setup() {
        if (!kafkaAdminService.topicExists(TOPIC)) {
            kafkaAdminService.createTopic(TOPIC, -1);
        }
    }

    @Test()
    public void given_messageThatHasToBeReprocessed_when_errorAppears_then_ConsumerProcessesItAgain() throws InterruptedException {

        //given
        producer.send(TOPIC, new BaseMessage(TOPIC, "This message will be correctly processed", "ExponentialBackoffMessageRetryIntegrationTest"));
        producer.send(TOPIC, new BaseMessage(TOPIC, "This message will NOT be correctly processed", "ExponentialBackoffMessageRetryIntegrationTest"));

        //when
        consumer.getIncorrectMessageLatch().await(10000, TimeUnit.MILLISECONDS);

        //then
        //incorrect message has been processed 2 times
        assertThat(consumer.getIncorrectMessageLatch().getCount()).isEqualTo(0);
        //correct message has been processed 1 time
        assertThat(consumer.getLatch().getCount()).isEqualTo(0);
    }
}
