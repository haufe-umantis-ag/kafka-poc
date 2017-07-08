package com.umantis.poc.model;

public class CommonMessage {

    private long time;
    private String topic;
    private String message;
    private String origin;
    private String customer;

    public CommonMessage() {
        super();
    }

    public CommonMessage(final String topic, final String message, final String origin, final String customerId) {
        super();
        this.topic = topic;
        this.message = message;
        this.origin = origin;
        this.customer = customerId;
        this.time = System.currentTimeMillis();
    }

    public CommonMessage(final long time, final String topic, final String message, final String origin, final String customerId) {
        this.time = time;
        this.topic = topic;
        this.message = message;
        this.origin = origin;
        this.customer = customerId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public long getTime() {
        return time;
    }

    public void setTime(final long time) {
        this.time = time;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(final String customer) {
        this.customer = customer;
    }

    @Override
    public String toString() {
        return "MyMessage{" +
                "time=" + time +
                ", topic='" + topic + '\'' +
                ", message='" + message + '\'' +
                ", origin='" + origin + '\'' +
                ", customer='" + customer + '\'' +
                '}';
    }

    public static BaseMessageBuilder builder() {
        return new BaseMessageBuilder();
    }

    public static class BaseMessageBuilder {

        private String message;
        private String origin;
        private String customerId;
        private String topic;

        BaseMessageBuilder() {
        }

        public CommonMessage.BaseMessageBuilder message(final String message) {
            this.message = message;
            return this;
        }

        public CommonMessage.BaseMessageBuilder origin(final String origin) {
            this.origin = origin;
            return this;
        }

        public CommonMessage.BaseMessageBuilder customerId(final String customerId) {
            this.customerId = customerId;
            return this;
        }

        public CommonMessage.BaseMessageBuilder topic(final String topic) {
            this.topic = topic;
            return this;
        }

        public CommonMessage build() {
            return new CommonMessage(origin, message, topic, customerId);
        }
    }
}
