package com.pivovarit.fencepost;

import java.time.Duration;
import java.util.Optional;

public interface FencepostLock extends AutoCloseable {

    FencingToken lock();

    FencingToken lock(Duration timeout);

    Optional<FencingToken> tryLock();

    void withLock(ThrowingConsumer<FencingToken> action);

    void withLock(Duration timeout, ThrowingConsumer<FencingToken> action);

    boolean isSuperseded(FencingToken token);

    void renew(Duration duration);

    void unlock();

    @Override
    void close();
}
