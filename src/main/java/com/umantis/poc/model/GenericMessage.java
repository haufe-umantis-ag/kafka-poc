package com.umantis.poc.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class implements a custom message header creation
 * This is a temporary solution until spring implements native kafka custom headers creation
 *
 * @param <T>
 */
public class GenericMessage<T> {

    private String origin;
    private String customer;
    private Map<String, String> headers = new HashMap<>();
    private T message;

    public GenericMessage() {
        super();
    }

    public GenericMessage(final String origin, final String customer, final Map<String, String> headers, final T message) {
        this.origin = origin;
        this.customer = customer;
        this.headers = headers;
        this.message = message;
    }

    public T getMessage() {
        return message;
    }

    public T readMessage(Class<T> genericType) {

        if ((message == null) || !(message instanceof LinkedHashMap)) {
            return null;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        T entity = null;
        LinkedHashMap linkedHashMap = (LinkedHashMap) message;

        try {
            String value = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(linkedHashMap);
            entity = objectMapper.readValue(value, genericType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entity;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public String getOrigin() {
        return origin;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(final String customer) {
        this.customer = customer;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(final Map<String, String> headers) {
        this.headers = headers;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static GenericMessageBuilder builder() {
        return new GenericMessageBuilder();
    }

    public static class GenericMessageBuilder<T> {

        private Map<Object, Object> headers;
        private String origin;
        private String customer;
        private T message;

        GenericMessageBuilder() {
        }

        public GenericMessageBuilder message(final T message) {
            this.message = message;
            return this;
        }

        public GenericMessageBuilder origin(final String origin) {
            this.origin = origin;
            return this;
        }

        public GenericMessageBuilder customer(final String customer) {
            this.customer = customer;
            return this;
        }

        public GenericMessageBuilder headers(final Map<Object, Object> headers) {
            this.headers = headers;
            return this;
        }

        public GenericMessage build() {
            return new GenericMessage(origin, customer, headers, message);
        }
    }
}
