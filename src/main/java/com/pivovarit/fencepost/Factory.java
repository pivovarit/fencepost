package com.pivovarit.fencepost;

import java.util.Objects;
import java.util.function.Function;

public class Factory<T> {

    private final Function<String, T> factory;

    protected Factory(Function<String, T> factory) {
        this.factory = factory;
    }

    public T forName(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return factory.apply(name);
    }
}
