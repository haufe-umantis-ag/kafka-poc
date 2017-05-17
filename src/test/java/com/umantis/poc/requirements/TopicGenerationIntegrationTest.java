package com.umantis.poc.requirements;

import com.umantis.poc.Producer;
import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TopicGenerationIntegrationTest {

    @Autowired
    public KafkaAdminUtils kafkaAdminService;

    @Autowired
    public Producer producer;

    private static String TOPIC;

    @Value("${kafka.non_existing_topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Test()
    public void given_topicDoesntExists_when_sendingAMessageToIt_then_theTopicIsAutomaticallyCreated() {

        //given
        boolean topicExists = kafkaAdminService.topicExists(TOPIC);
        Assert.isTrue(!topicExists, "Topic " + TOPIC + " not existing");

        //when
        BaseMessage message = BaseMessage.builder()
                .topic(TOPIC)
                .message("New topic " + TOPIC + " is created")
                .origin("TopicGenerationIntegrationTest")
                .customerId("0")
                .build();
        producer.send(TOPIC,message);
//        producer.send(TOPIC, new BaseMessage(TOPIC, "New topic " + TOPIC + " is created", "TopicGenerationIntegrationTest"));

        //then
        topicExists = kafkaAdminService.topicExists(TOPIC);
        Assert.isTrue(topicExists, "Topic " + TOPIC + " now exists");
    }

    @Before
    public void checkTopic() {
        //TOPIC was not finally deleted last time, delete.topic.enable propertie enabled?
        if (kafkaAdminService.topicExists(TOPIC)) {
            TOPIC = TOPIC + System.currentTimeMillis();
        }
    }

    @After
    public void tearDown() {
        kafkaAdminService.markTopicForDeletion(TOPIC);
    }
}
