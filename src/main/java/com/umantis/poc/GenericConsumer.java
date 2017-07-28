package com.umantis.poc;

import com.umantis.poc.model.GenericMessage;
import com.umantis.poc.model.NotificationMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author David Espinosa.
 */
public class GenericConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    private GenericMessage lastMessage;

    private CountDownLatch latch = new CountDownLatch(1);

    // TODO: implement custom converter to unmarshall the object directly
    @KafkaListener(id = "id2", topics = "#{kafkaTopicRandomGeneric}", containerFactory = "genericKafkaListenerContainerFactory")
    public void onMessage(final ConsumerRecord<String, String> consumerRecord) throws IOException {
        NotificationMessage notificationMessage = objectMapper.readValue(consumerRecord.value(), NotificationMessage.class);
        LOGGER.info("Received message {} from topic {}", notificationMessage, consumerRecord.topic());
        lastMessage = notificationMessage;
        latch.countDown();
    }

    // this only works using StringToJsonConverter
    //    @KafkaListener(id = "id2", topics = "#{kafkaTopicRandomGeneric}", containerFactory = "genericKafkaListenerContainerFactory")
    //    public void onMessage(NotificationMessage consumerRecord) {
    //        LOGGER.info("Received message {} from topic {}", consumerRecord, consumerRecord);
    //        lastMessage = consumerRecord;
    //        latch.countDown();
    //    }

    public GenericMessage getLastMessage() {
        return lastMessage;
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
