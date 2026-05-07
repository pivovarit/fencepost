package com.pivovarit.fencepost.queue;

import com.pivovarit.fencepost.Factory;

import java.util.function.Function;

public final class QueueFactory<T> extends Factory<T> {
    public QueueFactory(Function<String, T> factory) {
        super(factory);
    }
}
