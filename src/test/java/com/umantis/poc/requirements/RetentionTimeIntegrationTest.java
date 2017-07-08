package com.umantis.poc.requirements;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.umantis.poc.BaseTest;
import com.umantis.poc.Producer;
import com.umantis.poc.model.CommonMessage;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
public class RetentionTimeIntegrationTest extends BaseTest {

    @Autowired
    public Producer producer;

    private static long OLD_RETENTION_TIME = TimeUnit.DAYS.toMillis(3);
    private static long NEW_RETENTION_TIME = TimeUnit.DAYS.toMillis(7);

    private static String NEW_TOPIC = String.valueOf(System.currentTimeMillis());

    @Before
    public void setUp() {

        if (kafkaAdminService.topicExists(TOPIC)) {
            kafkaAdminService.setTopicRetentionTime(TOPIC, OLD_RETENTION_TIME);
        } else {
            kafkaAdminService.createTopic(TOPIC, OLD_RETENTION_TIME);
        }
    }

    @Test
    public void given_topicWithNoDataRetentionTimeSet_when_settingNewTime_then_theChangeRemains() {

        //given
        CommonMessage message = CommonMessage.builder()
                .topic(NEW_TOPIC)
                .message("New topic " + NEW_TOPIC + " is created")
                .origin("RetentionTimeIntegrationTest")
                .customerId("0")
                .build();
        producer.send(NEW_TOPIC, message);

        long topicRetentionTime = kafkaAdminService.getTopicRetentionTime(NEW_TOPIC);
        assertEquals(topicRetentionTime, -1);

        //when
        kafkaAdminService.setTopicRetentionTime(NEW_TOPIC, NEW_RETENTION_TIME);

        //then
        topicRetentionTime = kafkaAdminService.getTopicRetentionTime(NEW_TOPIC);
        assertEquals(topicRetentionTime, NEW_RETENTION_TIME);
    }

    @Test
    public void given_topicWithDataRetentionTimeSet_when_settingNewTime_then_theChangeRemains() {

        //given
        long topicRetentionTime = kafkaAdminService.getTopicRetentionTime(TOPIC);
        assertNotEquals(topicRetentionTime, TOPIC);

        //when
        kafkaAdminService.setTopicRetentionTime(TOPIC, NEW_RETENTION_TIME);

        //then
        topicRetentionTime = kafkaAdminService.getTopicRetentionTime(TOPIC);
        assertEquals(topicRetentionTime, NEW_RETENTION_TIME);
    }

    @After
    public void concreteTestTearDown() {
        try {
            kafkaAdminService.markTopicForDeletion(NEW_TOPIC);
        } catch (TopicAlreadyMarkedForDeletionException e) {
        }
    }
}
