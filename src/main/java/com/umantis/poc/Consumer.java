package com.umantis.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.concurrent.CountDownLatch;

/**
 * Basic spring kafka consumer
 *
 * @author David Espinosa.
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(id = "consumer", topics = "${kafka.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void receive(String message) {
        LOGGER.info("received message= "+message);
        latch.countDown();
    }

    public CountDownLatch latch() {
        return latch;
    }
}
