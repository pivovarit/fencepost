package com.pivovarit.fencepost.lock;

import com.pivovarit.fencepost.FencepostException;

public final class LockAcquisitionTimeoutException extends FencepostException {

    private static final long serialVersionUID = 1L;

    public LockAcquisitionTimeoutException(String lockName) {
        super("Timed out waiting to acquire lock: " + lockName);
    }
}

