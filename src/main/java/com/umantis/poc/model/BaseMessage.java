package com.umantis.poc.model;

public class BaseMessage {

  private String topic;
  private String message;
  private String origin;

  public BaseMessage() {
    super();
  }

  public BaseMessage(String topic, String message, String origin) {
    super();
    this.topic = topic;
    this.message = message;
    this.origin = origin;
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

  @Override
  public String toString() {
    return "Car [topic=" + topic + ", message=" + message + ", origin=" + origin + "]";
  }
}
