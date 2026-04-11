package com.pivovarit.fencepost.function;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(T t) throws Exception;
}
