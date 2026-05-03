package com.pivovarit.fencepost.queue;

import java.time.Duration;
import java.util.Map;

public interface QueuePublisher {
    void publish(byte[] payload, String type, Map<String, String> headers, Duration delay);

    default void publish(byte[] payload) {
        this.publish(payload, Duration.ZERO);
    }

    default void publish(byte[] payload, Duration delay) {
        this.publish(payload, null, Map.of());
    }

    default void publish(byte[] payload, String type, Map<String, String> headers) {
        this.publish(payload, type, headers, Duration.ZERO);
    }
}
