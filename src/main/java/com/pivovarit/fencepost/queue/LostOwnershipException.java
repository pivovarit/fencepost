package com.pivovarit.fencepost.queue;

import com.pivovarit.fencepost.FencepostException;

public class LostOwnershipException extends FencepostException {

    private static final long serialVersionUID = 1L;

    public LostOwnershipException(long messageId) {
        super("Lost ownership of message " + messageId + ": visibility expired and it was re-picked by another consumer");
    }
}
