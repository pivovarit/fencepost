package com.pivovarit.fencepost;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(T t) throws Exception;
}
