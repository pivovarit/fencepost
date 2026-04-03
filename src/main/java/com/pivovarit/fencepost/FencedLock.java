package com.pivovarit.fencepost;

import java.time.Duration;
import java.util.Optional;

public interface FencedLock extends FencepostLock {

    FencingToken fencedLock();

    FencingToken fencedLock(Duration timeout);

    Optional<FencingToken> tryFencedLock();

    default void withFencedLock(ThrowingConsumer<FencingToken> action) {
        FencingToken token = fencedLock();
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

    default void withFencedLock(Duration timeout, ThrowingConsumer<FencingToken> action) {
        FencingToken token = fencedLock(timeout);
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
