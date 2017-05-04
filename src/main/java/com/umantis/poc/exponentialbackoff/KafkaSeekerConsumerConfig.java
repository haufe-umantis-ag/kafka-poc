package com.umantis.poc.exponentialbackoff;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Producer Configuration with special setup for exponential backoff message retry consumer.
 * Autocommit is disabled in order to leave the commit responsibility to the consumer.
 *
 * @author David Espinosa.
 */
@Configuration
public class KafkaSeekerConsumerConfig {

    @Value("${kafka.servers}")
    private String servers;

    @Value("${seeker_consumer.grouip}")
    private String groupId;

    @Bean(name = "seekerConsumerConfig")
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return props;
    }

    @Bean(name = "seekerFactoryConfig")
    public ConsumerFactory<String, String> seekerFactoryConfig() {
        return new DefaultKafkaConsumerFactory<String, String>(consumerConfigs());
    }

    @Bean(name = "seekerKafkaFactoryConfig")
    public ConcurrentKafkaListenerContainerFactory<String, String> seekerKafkaFactoryConfig() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(seekerFactoryConfig());
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
        return factory;
    }

    @Bean
    public SeekerConsumer seekerConsumer() {
        return new SeekerConsumer();
    }
}
