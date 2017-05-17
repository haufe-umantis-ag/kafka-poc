package com.umantis.poc.requirements;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RetentionTimeIntegrationTest {

    @Autowired
    public KafkaAdminUtils kafkaAdminService;

    @Autowired
    public Producer producer;

    private static long OLD_RETENTION_TIME = TimeUnit.DAYS.toMillis(3);
    private static long NEW_RETENTION_TIME = TimeUnit.DAYS.toMillis(7);

    private static String NEW_TOPIC;
    private static String EXISTING_TOPIC;

    @Value("${kafka.retention_topic}")
    public void setTopic(String topic) {
        NEW_TOPIC = topic + "-new";
        EXISTING_TOPIC = topic + "-existing";
    }

    @Before
    public void setUp() {
        //NEW_TOPIC was not finally deleted last time, delete.topic.enable property enabled?
        if (kafkaAdminService.topicExists(NEW_TOPIC)) {
            NEW_TOPIC = NEW_TOPIC + "-" + System.currentTimeMillis();
        }
        if (kafkaAdminService.topicExists(EXISTING_TOPIC)) {
            kafkaAdminService.setTopicRetentionTime(EXISTING_TOPIC, OLD_RETENTION_TIME);
        } else {
            kafkaAdminService.createTopic(EXISTING_TOPIC, OLD_RETENTION_TIME);
        }
    }

    @Test
    public void given_topicWithNoDataRetentionTimeSet_when_settingNewTime_then_theChangeRemains() {

        //given
        BaseMessage message = BaseMessage.builder()
                .topic(NEW_TOPIC)
                .message("New topic " + NEW_TOPIC + " is created")
                .origin("RetentionTimeIntegrationTest")
                .customerId("0")
                .build();
        producer.send(NEW_TOPIC,message);

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
        long topicRetentionTime = kafkaAdminService.getTopicRetentionTime(EXISTING_TOPIC);
        assertNotEquals(topicRetentionTime, EXISTING_TOPIC);

        //when
        kafkaAdminService.setTopicRetentionTime(EXISTING_TOPIC, NEW_RETENTION_TIME);

        //then
        topicRetentionTime = kafkaAdminService.getTopicRetentionTime(EXISTING_TOPIC);
        assertEquals(topicRetentionTime, NEW_RETENTION_TIME);
    }

    @After
    public void tearDown() {
        kafkaAdminService.markTopicForDeletion(NEW_TOPIC);
    }
}
