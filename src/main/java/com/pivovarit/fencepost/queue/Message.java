package com.pivovarit.fencepost.queue;

import java.util.Map;
import java.util.Optional;

/**
 * A message dequeued from a {@link Queue}.
 *
 * <p>A message is held by its consumer for the queue's visibility timeout. Within that
 * window, the consumer must resolve it exactly once by calling {@link #ack()},
 * {@link #nack()}, or {@link #close()}. If none of those is called before the visibility
 * timeout expires, the broker assumes the consumer crashed and makes the message
 * eligible for redelivery to another consumer.
 *
 * <p>Instances are not thread-safe. Each message must be operated on by a single thread:
 * concurrent calls to {@link #ack()}, {@link #nack()}, or {@link #close()} from multiple
 * threads on the same instance are unsupported and may produce misleading exceptions.
 */
public interface Message extends AutoCloseable {

    /**
     * @return the message's unique identifier, stable across redeliveries.
     */
    long id();

    /**
     * @return the message body; never {@code null}.
     */
    byte[] payload();

    /**
     * @return the caller-supplied type tag, or {@link Optional#empty()} if the producer did
     * not set one.
     */
    Optional<String> type();

    /**
     * @return the caller-supplied headers as an unmodifiable map; empty if none were set.
     */
    Map<String, String> headers();

    /**
     * @return the number of times this message has been delivered, including the current
     * delivery. First delivery returns {@code 1}; increments on every subsequent redelivery
     * after a {@link #nack()} or visibility-timeout expiry.
     */
    int attempts();

    /**
     * Acknowledges successful processing and removes the message from the queue.
     *
     * @throws LostOwnershipException if the visibility timeout expired and the message was
     *                                re-picked by another consumer before this call reached
     *                                the database.
     * @throws AckUnknownException    if the database call itself failed, leaving the outcome
     *                                indeterminate. Local state is reset to active, so the
     *                                caller may retry or {@link #close()}.
     * @throws IllegalStateException  if this message has already been resolved locally via
     *                                {@code ack()}, {@link #nack()}, or {@link #close()}.
     */
    void ack();

    /**
     * Negatively acknowledges the message, making it immediately available for redelivery.
     * Use this to signal a transient failure that another consumer (or this one, on retry)
     * should retry without waiting for the visibility timeout.
     *
     * @throws LostOwnershipException if the visibility timeout expired and the message was
     *                                re-picked by another consumer before this call reached
     *                                the database.
     * @throws AckUnknownException    if the database call itself failed, leaving the outcome
     *                                indeterminate. Local state is reset to active, so the
     *                                caller may retry or {@link #close()}.
     * @throws IllegalStateException  if this message has already been resolved locally via
     *                                {@link #ack()}, {@code nack()}, or {@link #close()}.
     */
    void nack();

    /**
     * Releases local ownership without contacting the database. The message stays invisible
     * to other consumers until the visibility timeout elapses, then becomes eligible for
     * redelivery. Use this when you want to abandon the message without changing the
     * backoff. Idempotent and safe to call after {@link #ack()} or {@link #nack()}.
     */
    @Override
    void close();
}
