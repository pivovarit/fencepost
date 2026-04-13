package com.pivovarit.fencepost.lock;

import com.pivovarit.fencepost.FencepostException;

public class LockNotHeldException extends FencepostException {

    private static final long serialVersionUID = 1L;

    public LockNotHeldException(String lockName) {
        super("Cannot unlock, lock not held: " + lockName);
    }
}
