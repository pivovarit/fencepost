package com.pivovarit.fencepost.queue;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public interface Queue extends AutoCloseable {

    void enqueue(String payload);

    void enqueue(String payload, Duration delay);

    void enqueue(String payload, String type, Map<String, String> headers);

    void enqueue(String payload, String type, Map<String, String> headers, Duration delay);

    Message dequeue();

    Message dequeue(Duration timeout);

    Optional<Message> tryDequeue();

    @Override
    void close();
}
