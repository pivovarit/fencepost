package com.pivovarit.fencepost.lock;

import com.pivovarit.fencepost.Factory;

import java.util.function.Function;

public final class LockFactory<T extends FencepostLock> extends Factory<T> {
    public LockFactory(Function<String, T> factory) {
        super(factory);
    }
}
