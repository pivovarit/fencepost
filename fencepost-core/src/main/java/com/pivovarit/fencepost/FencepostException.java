package com.pivovarit.fencepost;

public class FencepostException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public FencepostException(String message) {
        super(message);
    }

    public FencepostException(String message, Throwable cause) {
        super(message, cause);
    }
}
