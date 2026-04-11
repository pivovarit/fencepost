package com.pivovarit.fencepost.lock;

import java.util.Objects;

public final class FencingToken implements Comparable<FencingToken> {

    private final long value;

    public FencingToken(long value) {
        this.value = value;
    }

    public long value() {
        return value;
    }

    @Override
    public int compareTo(FencingToken other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FencingToken)) {
            return false;
        }
        FencingToken that = (FencingToken) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "FencingToken{" + value + "}";
    }
}
