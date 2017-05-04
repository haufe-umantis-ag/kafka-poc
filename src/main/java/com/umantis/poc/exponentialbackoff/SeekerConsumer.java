package com.umantis.poc.exponentialbackoff;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
public class SeekerConsumer implements AcknowledgingMessageListener<Integer, String>, ConsumerSeekAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeekerConsumer.class);

    private ConsumerSeekCallback consumerSeekCallback;

    private CountDownLatch incorrectMessageLatch = new CountDownLatch(2);
    private CountDownLatch correctMessageLatch = new CountDownLatch(1);

    @Value("${kafka.seeker_topic}")
    private String seekerTopic;

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
    @KafkaListener(id = "seeker", topics = "${kafka.seeker_topic}", containerFactory = "seekerKafkaFactoryConfig")
    public void onMessage(final ConsumerRecord<Integer, String> consumerRecord, final Acknowledgment acknowledgment) {

        try {
            String value = (String) consumerRecord.value();
            if (value.contains("NOT")) {
                boolean emulateError = (incorrectMessageLatch.getCount() == 2);
                incorrectMessageLatch.countDown();
                if (emulateError) {
                    throw new RandomException("Random Exception to try message re-processing!");
                }
            } else {
                acknowledgment.acknowledge();
                correctMessageLatch.countDown();
            }
        } catch (RandomException e) {
            consumerSeekCallback.seek(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            LOGGER.error("Error processing message with offset: " + consumerRecord.offset() + " from topic: " + consumerRecord.topic());
        }
    }

    public CountDownLatch getIncorrectMessageLatch() {
        return incorrectMessageLatch;
    }

    public CountDownLatch getCorrectMessageLatch() {
        return correctMessageLatch;
    }
}
