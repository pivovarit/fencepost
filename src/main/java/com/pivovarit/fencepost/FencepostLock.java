package com.pivovarit.fencepost;

public interface FencepostLock extends AutoCloseable {

    void unlock();

    @Override
    void close();
}
