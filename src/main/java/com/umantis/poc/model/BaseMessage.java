package com.umantis.poc.model;

public class BaseMessage {

  private long time;
  private String topic;
  private String message;
  private String origin;
  private String customerId;

  public BaseMessage() {
    super();
  }

  public BaseMessage(final String topic, final String message, final String origin, final String customerId) {
    super();
    this.topic = topic;
    this.message = message;
    this.origin = origin;
    this.customerId = customerId;
    this.time = System.currentTimeMillis();
  }

  public BaseMessage(final long time, final String topic, final String message, final String origin, final String customerId) {
    this.time = time;
    this.topic = topic;
    this.message = message;
    this.origin = origin;
    this.customerId = customerId;
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

  @Override
  public String toString() {
    return "BaseMessage{" +
            "time=" + time +
            ", topic='" + topic + '\'' +
            ", message='" + message + '\'' +
            ", origin='" + origin + '\'' +
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

    public BaseMessage.BaseMessageBuilder message(final String message) {
      this.message = message;
      return this;
    }

    public BaseMessage.BaseMessageBuilder origin(final String origin) {
      this.origin = origin;
      return this;
    }

    public BaseMessage.BaseMessageBuilder customerId(final String customerId) {
      this.customerId = customerId;
      return this;
    }

    public BaseMessage.BaseMessageBuilder topic(final String topic) {
      this.topic = topic;
      return this;
    }

    public BaseMessage build() {
      return new BaseMessage(origin, message, topic, customerId);
    }
  }
}
