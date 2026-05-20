package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import com.pivovarit.fencepost.queue.AckUnknownException;
import com.pivovarit.fencepost.queue.LostOwnershipException;

public sealed class FencepostException extends RuntimeException
    permits LockNotHeldException, LockAcquisitionTimeoutException, LostOwnershipException, AckUnknownException {

    private static final long serialVersionUID = 1L;

    public FencepostException(String message) {
        super(message);
    }

    public FencepostException(String message, Throwable cause) {
        super(message, cause);
    }
}
