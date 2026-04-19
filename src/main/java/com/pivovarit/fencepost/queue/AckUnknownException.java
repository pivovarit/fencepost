package com.pivovarit.fencepost.queue;

import com.pivovarit.fencepost.FencepostException;

/**
 * Thrown when an {@link Message#ack()} or {@link Message#nack()} call fails because the
 * database call itself failed (e.g. connection lost, query error). The outcome is
 * indeterminate: the row may or may not have been updated.
 *
 * <p>Distinguishes "the DB call blew up" from "we definitively lost ownership"
 * ({@link LostOwnershipException}) and from "the caller already resolved this message locally"
 * ({@link IllegalStateException}).
 *
 * <p>After this is thrown, the message's local state is reset to active so the caller may
 * retry or {@link Message#close()} it. A retry of {@code ack()}/{@code nack()} is idempotent
 * at the row level: if the original operation actually landed, the retry will surface as
 * {@link LostOwnershipException}.
 */
public class AckUnknownException extends FencepostException {

    private static final long serialVersionUID = 1L;

    public AckUnknownException(long messageId, String operation, Throwable cause) {
        super("Outcome of " + operation + " for message " + messageId + " is unknown: database call failed", cause);
    }
}
