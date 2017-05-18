package com.umantis.poc.requirements;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.umantis.poc.Consumer;
import com.umantis.poc.Producer;
import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;

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

	@Value("#{kafkaTopicRandom}")
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
        BaseMessage correctMessage = BaseMessage.builder()
                .topic(TOPIC)
                .message("This message will be correctly processed")
                .origin("ExponentialBackoffMessageRetryIntegrationTest")
                .customerId("0")
                .build();
        producer.send(TOPIC, correctMessage);

        BaseMessage incorrectMessage = BaseMessage.builder()
                .topic(TOPIC)
                .message("This message will NOT be correctly processed")
                .origin("ExponentialBackoffMessageRetryIntegrationTest")
                .customerId("0")
                .build();
        producer.send(TOPIC, incorrectMessage);

        //when
        consumer.getIncorrectMessageLatch().await(10000, TimeUnit.MILLISECONDS);

        //then
        //incorrect message has been processed 2 times
        assertThat(consumer.getIncorrectMessageLatch().getCount()).isEqualTo(0);
        //correct message has been processed 1 time
        assertThat(consumer.getLatch().getCount()).isEqualTo(0);
    }
}
