package com.pivovarit.fencepost;

import java.util.Objects;
import java.util.function.Function;

public final class Factory<T> {

    private final Function<String, T> lockFactory;

    Factory(Function<String, T> lockFactory) {
        this.lockFactory = lockFactory;
    }

    public T forName(String lockName) {
        Objects.requireNonNull(lockName, "lockName must not be null");
        return lockFactory.apply(lockName);
    }
}
