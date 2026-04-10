package com.pivovarit.fencepost;

public class LockNotHeldException extends FencepostException {

    private static final long serialVersionUID = 1L;

    public LockNotHeldException(String lockName) {
        super("Cannot unlock, lock not held: " + lockName);
    }
}
