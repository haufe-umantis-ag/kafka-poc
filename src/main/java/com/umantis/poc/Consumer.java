package com.umantis.poc;

import com.umantis.poc.exponentialbackoff.RandomException;
import com.umantis.poc.model.CommonMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Consumer implementation for exponential backoff message retry
 *
 * @author David Espinosa.
 */
public class Consumer implements AcknowledgingMessageListener<String, CommonMessage>, ConsumerSeekAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private ConsumerSeekCallback consumerSeekCallback;

    private CountDownLatch incorrectMessageLatch = new CountDownLatch(2);
    private CountDownLatch correctMessageLatch = new CountDownLatch(1);

    public Consumer() {
        LOGGER.info("Consumer ready");
    }

    @Override
    public void registerSeekCallback(final ConsumerSeekCallback consumerSeekCallback) {
        this.consumerSeekCallback = consumerSeekCallback;
    }

    @Override
    public void onPartitionsAssigned(final Map<TopicPartition, Long> map, final ConsumerSeekCallback consumerSeekCallback) {

    }

    @Override
    public void onIdleContainer(final Map<TopicPartition, Long> map, final ConsumerSeekCallback consumerSeekCallback) {

    }

    @Override
    @KafkaListener(id = "id1", topics = "#{kafkaTopicRandom}")
    public void onMessage(final ConsumerRecord<String, CommonMessage> consumerRecord, final Acknowledgment acknowledgment) {

        try {
            CommonMessage value = (CommonMessage) consumerRecord.value();
            LOGGER.info("Received message {} from topic {}", value, consumerRecord.topic());
            if (value.getMessage().contains("NOT")) {
                boolean emulateError = (incorrectMessageLatch.getCount() == 2);
                incorrectMessageLatch.countDown();
                if (emulateError) {
                    throw new RandomException("Random Exception to try message re-processing!");
                }
            }

            correctMessageLatch.countDown();
            acknowledgment.acknowledge();
        } catch (RandomException e) {
            consumerSeekCallback.seek(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            LOGGER.error("Error processing message with offset: " + consumerRecord.offset() + " from topic: " + consumerRecord.topic());
        }
    }

    public CountDownLatch getIncorrectMessageLatch() {
        return incorrectMessageLatch;
    }

    public CountDownLatch getLatch() {
        return correctMessageLatch;
    }
}
