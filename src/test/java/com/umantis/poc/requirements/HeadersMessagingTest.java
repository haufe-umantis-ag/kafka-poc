package com.umantis.poc.requirements;

import com.umantis.poc.BaseTest;
import com.umantis.poc.GenericConsumer;
import com.umantis.poc.Producer;
import com.umantis.poc.model.GenericMessage;
import com.umantis.poc.model.NotificationMessage;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
public class HeadersMessagingTest extends BaseTest {

    @Autowired
    public Producer producer;

    @Autowired
    public GenericConsumer consumer;

    @Test
    public void given_messageWithHeadersIsSent_when_consumed_allDataIsRetrieved() throws Exception {

        Map<Object, Object> headers = new HashMap<>();
        headers.put("my_header1", "value 1");
        headers.put("my_header2", "value 2");

        NotificationMessage notificationEvent = new NotificationMessage("http://umantis/fake/endpoint", "created");
        GenericMessage message = GenericMessage.builder()
                .origin("HeadersMessagingTest")
                .customer("0")
                .message(notificationEvent)
                .headers(headers)
                .build();
        producer.sendGeneric(GENERIC_TOPIC, message);

        consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);

        GenericMessage lastMessage = consumer.getLastMessage();
        NotificationMessage receivedNotificationMessage = (NotificationMessage) lastMessage.readMessage(NotificationMessage.class);

        Assertions.assertThat(receivedNotificationMessage.equals(notificationEvent));
        Map receivedHeaders = lastMessage.getHeaders();
        Assertions.assertThat(receivedHeaders.equals(headers));
    }
}
