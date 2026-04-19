package com.pivovarit.fencepost.election;

/**
 * A sticky single-leader primitive: one of N instances wins a named role
 * and keeps it until it crashes or shuts down, at which point another
 * instance takes over within roughly one lease duration.
 *
 * <p>The instance that wins fires {@code onElected}; when leadership is
 * lost (auto-renew failure or graceful close) it fires {@code onRevoked}.
 * Both callbacks run on the same single internal daemon thread and must
 * return quickly — they are state-change notifications, not the place to
 * do real work. Real work should run on the user's own thread, gated by
 * {@link #isLeader()}.
 *
 * <p>Built on top of the lease lock; reuses the {@code fencepost_locks}
 * table.
 */
public interface LeaderElection extends AutoCloseable {

    /**
     * Starts the background election thread. Idempotent.
     *
     * @throws IllegalStateException if called after {@link #close()}.
     */
    void start();

    /**
     * Returns {@code true} if this instance currently holds the lease.
     * Cheap, lock-free read.
     */
    boolean isLeader();

    /**
     * Stops the election thread. If currently leader, fires
     * {@code onRevoked} synchronously and releases the lease before
     * returning. Idempotent. Never throws — errors are reported via the
     * configured {@code onCallbackError} handler.
     */
    @Override
    void close();
}
