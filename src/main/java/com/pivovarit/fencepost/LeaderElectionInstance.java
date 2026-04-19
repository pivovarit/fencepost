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

    private enum State { NEW, RUNNING, CLOSED }

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
    private volatile State state = State.NEW;
    private volatile boolean isLeader;
    private volatile boolean revokedSignal;
    private volatile RenewableLock currentLock;
    private volatile Thread loopThread;

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
            if (state == State.CLOSED) {
                throw new IllegalStateException("LeaderElection has been closed: " + electionName);
            }
            if (state == State.RUNNING) {
                return;
            }
            state = State.RUNNING;
            loopThread = new Thread(this::loop, "fencepost-leader-election-" + electionName);
            loopThread.setDaemon(true);
            loopThread.start();
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
            if (state == State.CLOSED) {
                return;
            }
            if (state == State.NEW) {
                state = State.CLOSED;
                return;
            }
            state = State.CLOSED;
            thread = loopThread;
        }
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        try {
            while (state != State.CLOSED) {
                AtomicBoolean live = new AtomicBoolean(true);
                RenewableLock lock = newLock(live);
                Optional<FencingToken> token;
                try {
                    token = lock.tryLock();
                } catch (Exception e) {
                    live.set(false);
                    logger.debug("tryLock failed for '{}' during standby polling", electionName, e);
                    reportError(e);
                    sleepInterruptibly(pollInterval);
                    continue;
                }

                if (token.isEmpty()) {
                    live.set(false);
                    sleepInterruptibly(pollInterval);
                    continue;
                }

                currentLock = lock;
                isLeader = true;
                FencingToken won = token.get();
                logger.debug("won leadership for '{}', token={}", electionName, won.value());
                invokeCallback(() -> onElected.accept(won));

                waitForRevocation();

                isLeader = false;
                invokeCallback(onRevoked);

                if (!revokedSignal) {
                    try {
                        lock.unlock();
                    } catch (Exception e) {
                        reportError(e);
                    }
                }
                currentLock = null;
                live.set(false);
                revokedSignal = false;
            }
        } finally {
            if (isLeader) {
                isLeader = false;
                invokeCallback(onRevoked);
            }
            RenewableLock lingering = currentLock;
            currentLock = null;
            if (lingering != null && !revokedSignal) {
                try {
                    lingering.unlock();
                } catch (Exception e) {
                    reportError(e);
                }
            }
        }
    }

    private RenewableLock newLock(AtomicBoolean live) {
        Fencepost.LeaseBuilder builder = Fencepost.leaseLock(dataSource, leaseDuration)
            .tableName(tableName)
            .withAutoRenew(renewInterval)
            .onAutoRenewFailure(e -> onRenewFailure(live, e))
            .withPollInterval(pollInterval);
        if (quietPeriod != null) {
            builder.withQuietPeriod(quietPeriod);
        }
        if (instanceId != null) {
            builder.withInstanceId(instanceId);
        }
        return builder.build().forName(electionName);
    }

    private void onRenewFailure(AtomicBoolean live, FencepostException e) {
        if (!live.get()) {
            logger.debug("ignoring stale renew failure for '{}'", electionName);
            return;
        }
        logger.debug("auto-renew failure for '{}': {}", electionName, e.getMessage());
        revokedSignal = true;
        Thread t = loopThread;
        if (t != null) {
            t.interrupt();
        }
    }

    private void waitForRevocation() {
        while (state != State.CLOSED && !revokedSignal) {
            LockSupport.park(this);
            // Clear interrupt flag set by close() or onRenewFailure so subsequent
            // sleeps in this thread don't throw spuriously.
            Thread.interrupted();
        }
    }

    private void sleepInterruptibly(Duration d) {
        if (state == State.CLOSED) {
            return;
        }
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            // close() interrupted us; loop condition will exit.
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
