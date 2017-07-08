package com.umantis.poc.requirements;

import com.umantis.poc.BaseTest;
import com.umantis.poc.Producer;
import com.umantis.poc.model.CommonMessage;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

/**
 * @author David Espinosa.
 */
public class TopicGenerationIntegrationTest extends BaseTest {

    @Autowired
    public Producer producer;

    private static String NON_EXISTING_TOPIC = "TopicGenerationIntegrationTest" + String.valueOf(System.currentTimeMillis());

    @Test()
    public void given_topicDoesNotExists_when_sendingAMessageToIt_then_theTopicIsAutomaticallyCreated() {

        //given
        boolean topicExists = kafkaAdminService.topicExists(NON_EXISTING_TOPIC);
        Assert.isTrue(!topicExists, "Topic " + TOPIC + " not existing");

        //when
        CommonMessage message = CommonMessage.builder()
                .topic(NON_EXISTING_TOPIC)
                .message("New topic " + NON_EXISTING_TOPIC + " is created")
                .origin("TopicGenerationIntegrationTest")
                .customerId("0")
                .build();
        producer.send(NON_EXISTING_TOPIC, message);

        //then
        topicExists = kafkaAdminService.topicExists(NON_EXISTING_TOPIC);
        Assert.isTrue(topicExists, "Topic " + NON_EXISTING_TOPIC + " now exists");
    }

    @After
    public void concreteTestTearDown() {
        kafkaAdminService.markTopicForDeletion(NON_EXISTING_TOPIC);
    }
}
