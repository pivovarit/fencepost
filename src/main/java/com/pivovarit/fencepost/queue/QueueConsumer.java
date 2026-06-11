package com.pivovarit.fencepost.queue;

/**
 * A managed consumer that continuously dequeues messages from a named queue
 * and dispatches them to a user-supplied handler.
 *
 * <p>On successful handler return the message is automatically acknowledged.
 * If the handler throws, the message is negatively acknowledged and becomes
 * visible again after the configured retry delay (1 second by default); when
 * a maximum delivery count is configured and exhausted, the message is
 * dead-lettered instead — kept in the queue table with {@code dead_at} and
 * {@code last_error} set, and never delivered again. Both knobs apply only to
 * handler throws: a handler that resolves the message itself (ack, nack, or
 * close) opts out of them for that delivery.
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
