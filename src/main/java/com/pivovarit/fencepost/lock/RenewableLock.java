package com.pivovarit.fencepost.lock;

import java.time.Duration;

public interface RenewableLock extends FencedLock {

    void renew(Duration duration);
}
