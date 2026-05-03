package com.pivovarit.fencepost.queue;

/**
 * A managed consumer that continuously dequeues messages from a named queue
 * and dispatches them to a user-supplied handler.
 *
 * <p>On successful handler return the message is automatically acknowledged;
 * if the handler throws, the message is nacked for immediate redelivery.
 *
 * <p>Lifecycle mirrors {@link com.pivovarit.fencepost.election.LeaderElection}:
 * call {@link #start()} to begin consuming and {@link #close()} to shut down
 * gracefully. {@code close()} waits for in-flight handler invocations to finish.
 */
public interface QueueConsumer extends AutoCloseable {

    /**
     * Starts the consumer threads. Idempotent.
     *
     * @throws IllegalStateException if called after {@link #close()}.
     */
    void start();

    /**
     * Stops all consumer threads. In-flight handler calls are allowed to
     * complete; queued messages that have not yet been dispatched are
     * released back to the queue via visibility-timeout expiry.
     * Idempotent. Never throws.
     */
    @Override
    void close();
}
