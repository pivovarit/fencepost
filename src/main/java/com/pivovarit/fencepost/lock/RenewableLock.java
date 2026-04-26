package com.pivovarit.fencepost.lock;

import java.time.Duration;

/**
 * Instances are not thread-safe. Each instance should be confined to a single thread.
 * For concurrent locking, create separate instances via {@code Factory.forName}.
 */
public interface RenewableLock extends FencedLock {

    void renew(Duration duration);
}
