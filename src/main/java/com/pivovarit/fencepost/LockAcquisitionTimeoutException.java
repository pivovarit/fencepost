package com.pivovarit.fencepost;

public class LockAcquisitionTimeoutException extends FencepostException {

    public LockAcquisitionTimeoutException(String lockName) {
        super("Timed out waiting to acquire lock: " + lockName);
    }
}

