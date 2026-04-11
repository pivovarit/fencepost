package com.pivovarit.fencepost;

import java.time.Duration;
import java.util.Optional;

public interface Queue extends AutoCloseable {

    void enqueue(String payload);

    void enqueue(String payload, Duration delay);

    Message dequeue();

    Message dequeue(Duration timeout);

    Optional<Message> tryDequeue();

    @Override
    void close();
}
