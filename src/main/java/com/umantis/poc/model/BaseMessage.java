package com.umantis.poc.model;

public class BaseMessage {

  private String make;
  private String manufacturer;
  private String id;

  public BaseMessage() {
    super();
  }

  public BaseMessage(String topic, String message, String origin) {
    super();
    this.make = topic;
    this.manufacturer = message;
    this.id = origin;
  }

  public String getMake() {
    return make;
  }

  public void setMake(String make) {
    this.make = make;
  }

  public String getManufacturer() {
    return manufacturer;
  }


  public void setManufacturer(String manufacturer) {
    this.manufacturer = manufacturer;
  }

  public String getId() {
    return id;
  }


  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "Car [topic=" + make + ", message=" + manufacturer + ", origin=" + id + "]";
  }
}
