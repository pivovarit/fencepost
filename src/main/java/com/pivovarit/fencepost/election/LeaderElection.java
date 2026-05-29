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
 * which thread delivers it. Callbacks must return quickly — they are
 * state-change notifications, not the place to do real work. Real work
 * should run on the user's own thread, gated by {@link #isLeader()}.
 *
 * <p>Built on top of the lease lock; reuses the {@code fencepost_locks}
 * table.
 *
 * <p><strong>{@code isLeader()} is advisory, not a mutual-exclusion
 * primitive.</strong> Leadership loss is detected by the auto-renew failing,
 * and that detection is guaranteed to complete strictly before the lease can
 * expire — so a standby cannot win while this instance still reports
 * leadership. That guarantee covers a hung or unreachable database for the
 * renew query itself; it cannot cover an unbounded pause of this JVM (for
 * example a long GC or the machine being suspended), during which the lease
 * may expire, a standby may be elected, and {@code isLeader()} may still
 * momentarily return {@code true} here. Therefore, for mutually exclusive
 * access to an <em>external</em> resource (a file, an API, another database),
 * do not gate writes on {@code isLeader()} alone. Carry the {@link
 * com.pivovarit.fencepost.lock.FencingToken} delivered to {@code onElected}
 * and have the protected resource reject any token older than the highest it
 * has seen. Only the monotonic fencing token provides correctness under
 * arbitrary pauses; {@code isLeader()} is a fast, best-effort hint for work
 * that is itself idempotent or token-gated.
 */
public interface LeaderElection extends AutoCloseable {

    /**
     * Starts the background election thread. Idempotent.
     *
     * @throws IllegalStateException if called after {@link #close()}.
     */
    void start();

    /**
     * Returns {@code true} if this instance currently believes it holds the
     * lease. Cheap, lock-free read. This is a best-effort hint, not a
     * cross-node lock: see the class-level note on fencing tokens before using
     * it to guard exclusive access to an external resource.
     */
    boolean isLeader();

    /**
     * Stops the election thread. If currently leader, fires
     * {@code onRevoked} synchronously and releases the lease before
     * returning. If the election thread does not exit within the join
     * timeout, {@code onRevoked} fires on the calling thread instead.
     * Idempotent. Never throws — errors are reported via the
     * configured {@code onCallbackError} handler.
     */
    @Override
    void close();
}
