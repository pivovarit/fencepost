package com.pivovarit.fencepost;

public interface Message extends AutoCloseable {

    long id();

    String payload();

    int attempts();

    void ack();

    void nack();

    @Override
    void close();
}
