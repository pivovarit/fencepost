package com.pivovarit.fencepost.election;

/**
 * A sticky single-leader primitive: one of N instances wins a named role
 * and keeps it until it crashes or shuts down, at which point another
 * instance takes over within roughly one lease duration.
 *
 * <p>The instance that wins fires {@code onElected}; when leadership is
 * lost (auto-renew failure or graceful close) it fires {@code onRevoked}.
 * Both callbacks normally run on the internal election daemon thread.
 * However, if {@link #close()} times out waiting for the election thread
 * to exit, {@code onRevoked} fires on the thread that called
 * {@code close()} instead. The callback fires at most once regardless of
 * which thread delivers it. Callbacks must return quickly - they are
 * state-change notifications, not the place to do real work. Real work
 * should run on the user's own thread, gated by {@link #isLeader()}.
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
     * returning. If the election thread does not exit within the join
     * timeout, {@code onRevoked} fires on the calling thread instead.
     * Idempotent. Never throws - errors are reported via the
     * configured {@code onCallbackError} handler.
     */
    @Override
    void close();
}
