package com.pivovarit.fencepost.queue;

import java.util.List;

/**
 * Administrative access to the dead-lettered messages of a single queue.
 * Stateless and thread-safe; there is nothing to close.
 */
public interface DeadLetters {

    /**
     * @param limit maximum number of messages to return, must be at least 1
     * @return dead-lettered messages, oldest first
     */
    List<DeadMessage> list(int limit);

    /**
     * @return the number of dead-lettered messages in this queue
     */
    long count();

    /**
     * Makes a dead-lettered message deliverable again: clears its dead marker and last error,
     * resets its delivery count to zero (so it gets a full {@code maxDeliveries} budget), and
     * wakes consumers waiting on the queue.
     *
     * @return {@code true} if the message was dead-lettered in this queue and has been redriven,
     *         {@code false} if no such dead-lettered message exists
     */
    boolean redrive(long id);

    /**
     * Redrives up to {@code max} dead-lettered messages, oldest first, exactly as {@link #redrive(long)}.
     *
     * @param max maximum number of messages to redrive, must be at least 1
     * @return the number of messages redriven
     */
    int redriveAll(int max);

    /**
     * Permanently deletes a dead-lettered message. Live messages are never deleted.
     *
     * @return {@code true} if the message was dead-lettered in this queue and has been deleted,
     *         {@code false} if no such dead-lettered message exists
     */
    boolean purge(long id);

    /**
     * Permanently deletes up to {@code max} dead-lettered messages, oldest first.
     *
     * @param max maximum number of messages to delete, must be at least 1
     * @return the number of messages deleted
     */
    int purgeAll(int max);
}
