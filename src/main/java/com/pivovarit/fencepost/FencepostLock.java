package com.pivovarit.fencepost;

import java.time.Duration;

public interface FencepostLock extends AutoCloseable {

    void lock();

    void lock(Duration timeout);

    boolean tryLock();

    default void withLock(Runnable action) {
        lock();
        try {
            action.run();
        } finally {
            unlock();
        }
    }

    default void withLock(Duration timeout, Runnable action) {
        lock(timeout);
        try {
            action.run();
        } finally {
            unlock();
        }
    }

    void unlock();

    @Override
    void close();
}
