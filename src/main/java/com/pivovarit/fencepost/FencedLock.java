package com.pivovarit.fencepost;

import java.time.Duration;
import java.util.Optional;

public interface FencedLock extends FencepostLock {

    FencingToken lock();

    FencingToken lock(Duration timeout);

    Optional<FencingToken> tryLock();

    default void withLock(ThrowingConsumer<FencingToken> action) {
        FencingToken token = lock();
        try {
            action.accept(token);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FencepostException("Action failed while holding lock", e);
        } finally {
            unlock();
        }
    }

    default void withLock(Duration timeout, ThrowingConsumer<FencingToken> action) {
        FencingToken token = lock(timeout);
        try {
            action.accept(token);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FencepostException("Action failed while holding lock", e);
        } finally {
            unlock();
        }
    }

    boolean isSuperseded(FencingToken token);
}
