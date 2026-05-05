package com.pivovarit.fencepost;

import com.pivovarit.fencepost.election.LeaderElection;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

final class LeaderElectionInstance implements LeaderElection {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionInstance.class);
    private static final long CLOSE_JOIN_TIMEOUT_MS = 10_000;

    private final String electionName;
    private final DataSource dataSource;
    private final String tableName;
    private final Duration leaseDuration;
    private final Duration renewInterval;
    private final Duration pollInterval;
    private final Duration quietPeriod;
    private final String instanceId;
    private final Consumer<FencingToken> onElected;
    private final Runnable onRevoked;
    private final Consumer<Throwable> onCallbackError;

    private final Object lifecycleLock = new Object();
    private final AtomicBoolean live = new AtomicBoolean(false);
    private final AtomicBoolean revoked = new AtomicBoolean(false);
    private final AtomicBoolean revokedPending = new AtomicBoolean(false);

    private volatile boolean closed;
    private volatile boolean isLeader;
    private volatile Thread executor;

    LeaderElectionInstance(String electionName,
                           DataSource dataSource,
                           String tableName,
                           Duration leaseDuration,
                           Duration renewInterval,
                           Duration pollInterval,
                           Duration quietPeriod,
                           String instanceId,
                           Consumer<FencingToken> onElected,
                           Runnable onRevoked,
                           Consumer<Throwable> onCallbackError) {
        this.electionName = electionName;
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.leaseDuration = leaseDuration;
        this.renewInterval = Objects.requireNonNull(renewInterval, "renewInterval must not be null");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval must not be null");
        this.quietPeriod = quietPeriod;
        this.instanceId = instanceId;
        this.onElected = onElected;
        this.onRevoked = onRevoked;
        this.onCallbackError = onCallbackError;
    }

    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("LeaderElection has been closed: " + electionName);
            }
            if (executor != null) {
                return;
            }
            executor = new Thread(this::electionLoop, "fencepost-leader-election-" + electionName);
            executor.setDaemon(true);
            executor.start();
        }
    }

    @Override
    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public void close() {
        Thread thread;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            thread = executor;
        }
        if (thread != null) {
            LockSupport.unpark(thread);
            if (Thread.currentThread() != thread) {
                try {
                    thread.join(CLOSE_JOIN_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (thread.isAlive()) {
                    logger.warn("election thread for '{}' did not exit within {} ms; detaching (daemon)", electionName, CLOSE_JOIN_TIMEOUT_MS);
                    isLeader = false;
                    if (revokedPending.compareAndSet(true, false)) {
                        invokeCallback(onRevoked);
                    }
                    thread.interrupt();
                }
            }
        }
    }

    private void electionLoop() {
        RenewableLock lock = newLock();
        while (!closed) {
            lock = runOneCycle(lock);
        }
    }

    private RenewableLock runOneCycle(RenewableLock lock) {
        live.set(true);
        revoked.set(false);
        try {
            Optional<FencingToken> token = tryAcquireLock(lock);
            if (token.isEmpty()) {
                sleep(pollInterval);
                return lock;
            }

            FencingToken won = token.get();
            logger.debug("won leadership for '{}', token={}", electionName, won.value());
            isLeader = true;
            revokedPending.set(true);
            try {
                invokeCallback(() -> onElected.accept(won));
                waitForRevocation(revoked);
            } finally {
                isLeader = false;
                if (revokedPending.compareAndSet(true, false)) {
                    invokeCallback(onRevoked);
                }
                try {
                    lock.unlock();
                } catch (Exception e) {
                    if (!revoked.get()) {
                        reportError(e);
                    }
                }
            }
        } finally {
            live.set(false);
        }
        if (revoked.get()) {
            return newLock();
        }
        return lock;
    }

    private Optional<FencingToken> tryAcquireLock(RenewableLock lock) {
        try {
            return lock.tryLock();
        } catch (Exception e) {
            logger.debug("tryLock failed for '{}' during standby polling", electionName, e);
            reportError(e);
        }
        return Optional.empty();
    }

    private RenewableLock newLock() {
        Fencepost.Locks.LeaseBuilder builder = Fencepost.Locks.lease(dataSource, leaseDuration)
            .tableName(tableName)
            .withAutoRenew(renewInterval)
            .onAutoRenewFailure(this::onRenewFailure)
            .withPollInterval(pollInterval);
        if (quietPeriod != null) {
            builder.withQuietPeriod(quietPeriod);
        }
        if (instanceId != null) {
            builder.withInstanceId(instanceId);
        }
        return builder.build().forName(electionName);
    }

    private void onRenewFailure(FencepostException e) {
        if (!live.get()) {
            logger.debug("ignoring stale renew failure for '{}'", electionName);
            return;
        }
        logger.debug("auto-renew failure for '{}': {}", electionName, e.getMessage());
        revoked.set(true);
        Thread t = executor;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    private void waitForRevocation(AtomicBoolean revoked) {
        while (!closed && !revoked.get()) {
            LockSupport.park(this);
        }
    }

    private void sleep(Duration d) {
        long deadline = System.nanoTime() + d.toNanos();
        while (!closed) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(this, remaining);
        }
    }

    private void invokeCallback(Runnable callback) {
        try {
            callback.run();
        } catch (Throwable t) {
            reportError(t);
        }
    }

    private void reportError(Throwable t) {
        try {
            onCallbackError.accept(t);
        } catch (Throwable inner) {
            logger.warn("onCallbackError handler itself threw for '{}'", electionName, inner);
        }
    }
}
