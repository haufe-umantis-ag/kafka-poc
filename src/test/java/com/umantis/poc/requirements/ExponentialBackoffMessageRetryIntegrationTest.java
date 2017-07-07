package com.umantis.poc.requirements;

import static org.assertj.core.api.Assertions.assertThat;

import com.umantis.poc.BaseTest;
import com.umantis.poc.Consumer;
import com.umantis.poc.Producer;
import com.umantis.poc.model.BaseMessage;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
public class ExponentialBackoffMessageRetryIntegrationTest extends BaseTest {

    @Autowired
    public Producer producer;

    @Autowired
    public Consumer consumer;

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
