package com.pivovarit.fencepost;

import java.time.Duration;

/**
 * Instances are not thread-safe. Each instance should be confined to a single thread.
 * For concurrent locking, create separate instances via the {@link AdvisoryLockProvider}.
 */
public interface AdvisoryLock extends FencepostLock {

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
}
