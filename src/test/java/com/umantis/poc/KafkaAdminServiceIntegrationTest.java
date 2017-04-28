package com.umantis.poc;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaAdminServiceIntegrationTest {

    @Autowired
    public KafkaAdminUtils kafkaAdminService;

    @Autowired
    public KafkaProducer producer;

    private static long NEW_RETENTION_TIME = TimeUnit.DAYS.toMillis(7);

    private static String TOPIC;
    private static String TOPIC_RETENTION;

    @Value("${kafka.non_existing_topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
        TOPIC_RETENTION = topic + "-retention";
    }

    @Test()
    public void given_topicDoesntExists_when_sendingAMessageToIt_then_theTopicIsAutomaticallyCreated() {

        //given
        boolean topicExists = kafkaAdminService.topicExists(TOPIC);
        Assert.isTrue(!topicExists, "Topic " + TOPIC + " not existing");

        //when
        producer.send(TOPIC, "New topic " + TOPIC + " is created");

        //then
        topicExists = kafkaAdminService.topicExists(TOPIC);
        Assert.isTrue(topicExists, "Topic " + TOPIC + " now exists");
    }

    @Test
    public void given_topicWithNoDataRetentionTimeSet_when_changingIt_then_theChangeRemains() {

        //given
        producer.send(TOPIC_RETENTION, "New topic " + TOPIC_RETENTION + " is created");
        long topicRetentionTime = kafkaAdminService.getTopicRetentionTime(TOPIC_RETENTION);
        assertEquals(topicRetentionTime, -1);

        //when
        kafkaAdminService.setTopicRetentionTime(TOPIC_RETENTION, NEW_RETENTION_TIME);

        //then
        topicRetentionTime = kafkaAdminService.getTopicRetentionTime(TOPIC_RETENTION);
        assertEquals(topicRetentionTime, NEW_RETENTION_TIME);
    }

    @Before
    public void checkTopic() {
        //TOPIC was not finally deleted last time, delete.topic.enable propertie enabled?
        if (kafkaAdminService.topicExists(TOPIC)) {
            TOPIC = TOPIC + System.currentTimeMillis();
        }
        if (kafkaAdminService.topicExists(TOPIC_RETENTION)) {
            TOPIC_RETENTION = TOPIC + "-retention";
        }
    }

    @After
    public void tearDown() {
        kafkaAdminService.markTopicForDeletion(TOPIC);
        kafkaAdminService.markTopicForDeletion(TOPIC_RETENTION);
    }
}
