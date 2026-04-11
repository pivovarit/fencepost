package com.pivovarit.fencepost.queue;

import java.util.Map;

public interface Message extends AutoCloseable {

    long id();

    String payload();

    String type();

    Map<String, String> headers();

    int attempts();

    void ack();

    void nack();

    @Override
    void close();
}
