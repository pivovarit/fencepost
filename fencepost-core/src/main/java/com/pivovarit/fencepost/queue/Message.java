package com.pivovarit.fencepost.queue;

import java.util.Map;

public interface Message extends AutoCloseable {

    long id();

    byte[] payload();

    String type();

    Map<String, String> headers();

    int attempts();

    void ack();

    void nack();

    @Override
    void close();
}
