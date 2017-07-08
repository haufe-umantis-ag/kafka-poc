package com.umantis.poc;

import com.umantis.poc.model.GenericMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.concurrent.CountDownLatch;

/**
 * @author David Espinosa.
 */
public class GenericConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericConsumer.class);

    private GenericMessage lastMessage;

    private CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(id = "id2", topics = "#{kafkaTopicRandomGeneric}", containerFactory = "genericKafkaListenerContainerFactory")
    public void onMessage(final ConsumerRecord<String, GenericMessage> consumerRecord) {
        GenericMessage value = (GenericMessage) consumerRecord.value();
        LOGGER.info("Received message {} from topic {}", value, consumerRecord.topic());
        lastMessage = value;
        latch.countDown();
    }

    public GenericMessage getLastMessage() {
        return lastMessage;
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
