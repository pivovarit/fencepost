package com.pivovarit.fencepost;

@FunctionalInterface
public interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
}
