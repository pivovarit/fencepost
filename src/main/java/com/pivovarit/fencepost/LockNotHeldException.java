package com.pivovarit.fencepost;

public class LockNotHeldException extends FencepostException {

    public LockNotHeldException(String lockName) {
        super("Cannot unlock, lock not held: " + lockName);
    }
}
