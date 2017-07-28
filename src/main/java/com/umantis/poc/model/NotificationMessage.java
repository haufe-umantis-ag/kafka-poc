package com.umantis.poc.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import java.util.HashMap;
import java.util.Map;

/**
 * @author David Espinosa.
 */
@Data
@ToString
@NoArgsConstructor
public class NotificationMessage extends GenericMessage {

    private String url;
    private String action;

    public NotificationMessage(final Map<String, Object> headers, final String resourceId, String url, String action) {
        super(headers, resourceId);
        this.url = url;
        this.action = action;
    }

    public static NotificationMessageBuilder builder() {
        return new NotificationMessageBuilder();
    }

    public static class NotificationMessageBuilder implements GenericMessageBuilderInterface<NotificationMessageBuilder> {

        private Map<String, Object> headers;
        private String url;
        private String action;
        private String resourceId;

        public NotificationMessageBuilder setUrl(final String url) {
            this.url = url;
            return this;
        }

        public NotificationMessageBuilder setAction(final String action) {
            this.action = action;
            return this;
        }

        @Override
        public NotificationMessageBuilder setHeaders(final Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        @Override
        public NotificationMessageBuilder addHeader(final String key, final Object value) {
            if (headers == null) {
                headers = new HashMap<>();
            }
            headers.put(key, value);
            return this;
        }

        @Override
        public NotificationMessageBuilder setResourceId(final String resourceId) {
            this.resourceId = resourceId;
            return this;
        }

        public NotificationMessage build() {
            return new NotificationMessage(this.headers, this.resourceId, this.url, this.action);
        }
    }
}
