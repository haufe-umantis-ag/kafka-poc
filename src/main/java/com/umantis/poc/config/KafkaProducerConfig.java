package com.umantis.poc.config;

import com.umantis.poc.Producer;
import com.umantis.poc.model.CommonMessage;
import com.umantis.poc.model.GenericMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic Kafka configuration for producer.
 *
 * @author David Espinosa.
 */
@Configuration
public class KafkaProducerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Value("${kafka.servers}")
    private String servers;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, CommonMessage> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs(), new StringSerializer(), new JsonSerializer(new ObjectMapper()));
    }

    @Bean
    public KafkaTemplate<String, CommonMessage> kafkaTemplate() {
        return new KafkaTemplate<String, CommonMessage>(producerFactory());
    }

    @Bean
    public <T> ProducerFactory<String, GenericMessage<T>> genericProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs(), new StringSerializer(), new JsonSerializer(new ObjectMapper()));
    }

    @Bean
    public <T> KafkaTemplate<String, GenericMessage<T>> genericKafkaTemplate() {
        return new KafkaTemplate<String, GenericMessage<T>>(genericProducerFactory());
    }

    @Bean
    public Producer producer() {
        LOGGER.info("Creating producer");
        return new Producer();
    }
}
