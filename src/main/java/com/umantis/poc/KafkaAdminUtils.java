package com.umantis.poc;

import java.util.List;

/**
 * @author David Espinosa.
 */
public interface KafkaAdminUtils {

    int ACL_CRUD_IDENTIFIER = 31;
    String ACL_ALL_SCHEMAS = "world";
    String ACL_ANYONE_ID = "anyone";
    String KAFKA_RETENTION_TIME_PROPERTY = "retention.ms";

    void markTopicForDeletion(String topic);

    long getTopicRetentionTime(String topic);

    void setTopicRetentionTime(String topic, long retentionTimeInMs);

    boolean topicExists(String topic);

    List<String> listTopics();
}
