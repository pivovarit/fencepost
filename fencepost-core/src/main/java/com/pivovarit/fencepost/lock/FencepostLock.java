package com.pivovarit.fencepost.lock;

public interface FencepostLock extends AutoCloseable {

    void unlock();

    @Override
    void close();
}
