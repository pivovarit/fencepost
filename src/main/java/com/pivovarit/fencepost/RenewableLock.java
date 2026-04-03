package com.pivovarit.fencepost;

import java.time.Duration;

public interface RenewableLock extends FencedLock {

    void renew(Duration duration);
}
