package com.pivovarit.fencepost.lock;

public record FencingToken(long value) implements Comparable<FencingToken> {

    @Override
    public int compareTo(FencingToken other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public String toString() {
        return "FencingToken{" + value + "}";
    }
}
