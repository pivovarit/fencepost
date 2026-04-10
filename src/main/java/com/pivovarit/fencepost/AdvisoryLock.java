package com.pivovarit.fencepost;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Instances are not thread-safe. Each instance should be confined to a single thread.
 * For concurrent locking, create separate instances via the {@link Fencepost.AdvisoryBuilder}.
 */
public interface AdvisoryLock extends FencepostLock {

    void lock();

    void lock(Duration timeout);

    boolean tryLock();

    default void runLocked(Runnable action) {
        lock();
        try {
            action.run();
        } finally {
            unlock();
        }
    }

    default <T> T supplyLocked(Supplier<T> action) {
        lock();
        try {
            return action.get();
        } finally {
            unlock();
        }
    }

    default void runLocked(Duration timeout, Runnable action) {
        lock(timeout);
        try {
            action.run();
        } finally {
            unlock();
        }
    }

    default <T> T supplyLocked(Duration timeout, Supplier<T> action) {
        lock(timeout);
        try {
            return action.get();
        } finally {
            unlock();
        }
    }
}
