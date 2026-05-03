package com.pivovarit.fencepost.queue;

import java.time.Duration;
import java.util.Map;

public interface QueuePublisher {
    void publish(byte[] payload);

    void publish(byte[] payload, Duration delay);

    void publish(byte[] payload, String type, Map<String, String> headers);

    void publish(byte[] payload, String type, Map<String, String> headers, Duration delay);
}
