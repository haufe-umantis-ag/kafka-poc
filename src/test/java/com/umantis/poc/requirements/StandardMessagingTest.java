package com.umantis.poc.requirements;

import com.umantis.poc.BaseTest;
import com.umantis.poc.Consumer;
import com.umantis.poc.Producer;
import com.umantis.poc.model.CommonMessage;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
public class StandardMessagingTest extends BaseTest {

    @Autowired
    public Producer producer;

    @Autowired
    public Consumer consumer;

    @Test
    public void testReceive() throws Exception {

        CommonMessage message = CommonMessage.builder()
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
