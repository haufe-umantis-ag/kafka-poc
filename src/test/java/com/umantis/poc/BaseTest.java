package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public abstract class BaseTest {

    protected static String TOPIC;
    protected static String GENERIC_TOPIC;

    protected static KafkaAdminUtils kafkaAdminService;

    @Autowired
    public void setKafAdminUtils(KafkaAdminUtils kafAdminUtils) {
        kafkaAdminService = kafAdminUtils;
    }

    @Autowired
    @Qualifier("kafkaTopicRandom")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Autowired
    @Qualifier("kafkaTopicRandomGeneric")
    public void setTopicGeneric(String topic) {
        GENERIC_TOPIC = topic;
    }

    // application kafka listeners will thrown an error as they will loose synchronicity with the deleted topics
    // as expected after this tearDown is executed
    // an global application ExceptionHandler could be used to avoid those verbose exception messages
    @AfterClass
    public static void tearDown() {
        try {
            kafkaAdminService.markTopicForDeletion(TOPIC);
        } catch (TopicAlreadyMarkedForDeletionException e) {
        }

        try {
            kafkaAdminService.markTopicForDeletion(GENERIC_TOPIC);
        } catch (TopicAlreadyMarkedForDeletionException e) {
        }
    }
}
