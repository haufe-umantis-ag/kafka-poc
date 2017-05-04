package com.umantis.poc.requirements;

import static org.assertj.core.api.Assertions.assertThat;

import com.umantis.poc.Producer;
import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.exponentialbackoff.SeekerConsumer;
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
    public SeekerConsumer seekerConsumer;

    private static String TOPIC;

    @Value("${kafka.seeker_topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Test()
    public void given_messageThatHasToBeReprocessed_when_errorAppears_then_ConsumerProcessesItAgain() throws InterruptedException {

        //given
        producer.send(TOPIC, "This message will be correctly processed");
        producer.send(TOPIC, "This message will NOT be correctly processed");

        //when
        seekerConsumer.getIncorrectMessageLatch().await(10000, TimeUnit.MILLISECONDS);

        //then
        //incorrect message has been processed 2 times
        assertThat(seekerConsumer.getIncorrectMessageLatch().getCount()).isEqualTo(0);
        //correct message has been processed 1 time
        assertThat(seekerConsumer.getCorrectMessageLatch().getCount()).isEqualTo(0);
    }
}
