package com.pivovarit.fencepost;

public class FencepostException extends RuntimeException {

    public FencepostException(String message) {
        super(message);
    }

    public FencepostException(String message, Throwable cause) {
        super(message, cause);
    }
}
