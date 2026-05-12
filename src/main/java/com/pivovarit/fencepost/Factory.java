package com.pivovarit.fencepost;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

public class Factory<T> {

    private static final Pattern VALID_NAME = Pattern.compile("[a-z0-9][a-z0-9_-]{0,48}");

    private final Function<String, T> factory;

    protected Factory(Function<String, T> factory) {
        this.factory = factory;
    }

    public T forName(String name) {
        Objects.requireNonNull(name, "name must not be null");
        if (!VALID_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("name must be 1-49 characters, lowercase alphanumeric, hyphens, and underscores: '" + name + "'");
        }
        return factory.apply(name);
    }
}
