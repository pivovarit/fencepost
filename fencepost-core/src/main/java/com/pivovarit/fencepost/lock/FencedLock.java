package com.pivovarit.fencepost.lock;

import com.pivovarit.fencepost.FencepostException;
import com.pivovarit.fencepost.function.ThrowingConsumer;
import com.pivovarit.fencepost.function.ThrowingFunction;

import java.time.Duration;
import java.util.Optional;

public interface FencedLock extends FencepostLock {

    FencingToken lock();

    FencingToken lock(Duration timeout);

    Optional<FencingToken> tryLock();

    default void runLocked(ThrowingConsumer<FencingToken> action) {
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

    default <T> T supplyLocked(ThrowingFunction<FencingToken, T> action) {
        FencingToken token = lock();
        try {
            return action.apply(token);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FencepostException("Action failed while holding lock", e);
        } finally {
            unlock();
        }
    }

    default void runLocked(Duration timeout, ThrowingConsumer<FencingToken> action) {
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

    default <T> T supplyLocked(Duration timeout, ThrowingFunction<FencingToken, T> action) {
        FencingToken token = lock(timeout);
        try {
            return action.apply(token);
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
