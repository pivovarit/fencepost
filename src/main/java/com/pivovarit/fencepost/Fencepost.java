package com.pivovarit.fencepost;

import com.pivovarit.fencepost.election.LeaderElection;
import com.pivovarit.fencepost.function.ThrowingConsumer;
import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import com.pivovarit.fencepost.queue.QueueConsumer;
import com.pivovarit.fencepost.queue.QueueFactory;
import com.pivovarit.fencepost.queue.QueuePublisher;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public final class Fencepost {

    static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*");

    private Fencepost() {
    }

    static void validateTableName(String tableName) {
        Objects.requireNonNull(tableName);
        if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }

    public static final class Locks {

        private Locks() {
        }

        public static AdvisoryBuilder advisory(DataSource dataSource) {
            return new AdvisoryBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
        }

        public static SessionBuilder session(DataSource dataSource) {
            return new SessionBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
        }

        public static LeaseBuilder lease(DataSource dataSource, Duration lockAtMost) {
            Objects.requireNonNull(dataSource, "dataSource must not be null");
            Durations.requireAtLeastOneMillisecond(lockAtMost, "lockAtMost");
            return new LeaseBuilder(dataSource, lockAtMost);
        }

        public static LeaderElectionBuilder leaderElection(DataSource dataSource, String electionName, Duration leaseDuration) {
            Objects.requireNonNull(dataSource, "dataSource must not be null");
            Objects.requireNonNull(electionName, "electionName must not be null");
            if (electionName.isEmpty()) {
                throw new IllegalArgumentException("electionName must not be empty");
            }
            Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
            Durations.requireAtLeastOneMillisecond(leaseDuration, "leaseDuration");
            return new LeaderElectionBuilder(dataSource, electionName, leaseDuration);
        }

        public static final class AdvisoryBuilder {
            private final DataSource dataSource;

            private AdvisoryBuilder(DataSource dataSource) {
                this.dataSource = dataSource;
            }

            public LockFactory<AdvisoryLock> build() {
                return new LockFactory<>(lockName -> new AdvisoryLockInstance(lockName, dataSource));
            }
        }

        public static final class SessionBuilder {
            private final DataSource dataSource;
            private String tableName = "fencepost_locks";
            private SchemaMode schemaMode = SchemaMode.NONE;

            private SessionBuilder(DataSource dataSource) {
                this.dataSource = dataSource;
            }

            public SessionBuilder tableName(String tableName) {
                validateTableName(tableName);
                this.tableName = tableName;
                return this;
            }

            public SessionBuilder schemaMode(SchemaMode schemaMode) {
                this.schemaMode = Objects.requireNonNull(schemaMode, "schemaMode must not be null");
                return this;
            }

            public LockFactory<FencedLock> build() {
                schemaMode.applyToLocks(dataSource, tableName);
                String t = this.tableName;
                return new LockFactory<>(lockName -> new SessionLockInstance(lockName, dataSource, t));
            }
        }

        public static final class LeaseBuilder {
            private final DataSource dataSource;
            private final Duration leaseDuration;
            private String tableName = "fencepost_locks";
            private Duration refreshInterval;
            private Duration quietPeriod;
            private Duration pollInterval;
            private Consumer<FencepostException> onAutoRenewFailure;
            private String instanceId;
            private SchemaMode schemaMode = SchemaMode.NONE;

            private LeaseBuilder(DataSource dataSource, Duration leaseDuration) {
                this.dataSource = dataSource;
                this.leaseDuration = leaseDuration;
            }

            public LeaseBuilder tableName(String tableName) {
                validateTableName(tableName);
                this.tableName = tableName;
                return this;
            }

            public LeaseBuilder withAutoRenew(Duration refreshInterval) {
                Durations.requireAtLeastOneMillisecond(refreshInterval, "Refresh interval");
                if (refreshInterval.compareTo(leaseDuration) >= 0) {
                    throw new IllegalArgumentException("Refresh interval must be less than lease duration");
                }
                this.refreshInterval = refreshInterval;
                return this;
            }

            public LeaseBuilder withQuietPeriod(Duration quietPeriod) {
                Durations.requireAtLeastOneMillisecond(quietPeriod, "quietPeriod");
                this.quietPeriod = quietPeriod;
                return this;
            }

            public LeaseBuilder withPollInterval(Duration pollInterval) {
                Durations.requireAtLeastOneMillisecond(pollInterval, "Poll interval");
                this.pollInterval = pollInterval;
                return this;
            }

            public LeaseBuilder onAutoRenewFailure(Consumer<FencepostException> handler) {
                this.onAutoRenewFailure = Objects.requireNonNull(handler);
                return this;
            }

            public LeaseBuilder withInstanceId(String instanceId) {
                Objects.requireNonNull(instanceId, "instanceId must not be null");
                if (instanceId.isEmpty()) {
                    throw new IllegalArgumentException("instanceId must not be empty");
                }
                this.instanceId = instanceId;
                return this;
            }

            public LeaseBuilder schemaMode(SchemaMode schemaMode) {
                this.schemaMode = Objects.requireNonNull(schemaMode, "schemaMode must not be null");
                return this;
            }

            public LockFactory<RenewableLock> build() {
                schemaMode.applyToLocks(dataSource, this.tableName);
                return new LockFactory<>(lockName -> new LeaseLockInstance(lockName, dataSource,
                  this.tableName,
                  this.leaseDuration,
                  this.refreshInterval,
                  this.quietPeriod,
                  this.pollInterval,
                  this.onAutoRenewFailure,
                  this.instanceId));
            }
        }

        public static final class LeaderElectionBuilder {
            private final DataSource dataSource;
            private final String electionName;
            private final Duration leaseDuration;
            private String tableName = "fencepost_locks";
            private Duration renewInterval;
            private Duration pollInterval;
            private Duration quietPeriod;
            private String instanceId;
            private Consumer<FencingToken> onElected = token -> {};
            private Runnable onRevoked = () -> {};
            private Consumer<Throwable> onCallbackError = t -> {};
            private SchemaMode schemaMode = SchemaMode.NONE;

            private LeaderElectionBuilder(DataSource dataSource, String electionName, Duration leaseDuration) {
                this.dataSource = dataSource;
                this.electionName = electionName;
                this.leaseDuration = leaseDuration;
            }

            public LeaderElectionBuilder tableName(String tableName) {
                validateTableName(tableName);
                this.tableName = tableName;
                return this;
            }

            public LeaderElectionBuilder withRenewInterval(Duration renewInterval) {
                Durations.requireAtLeastOneMillisecond(renewInterval, "renewInterval");
                if (renewInterval.compareTo(leaseDuration) >= 0) {
                    throw new IllegalArgumentException("renewInterval must be less than leaseDuration");
                }
                this.renewInterval = renewInterval;
                return this;
            }

            public LeaderElectionBuilder withPollInterval(Duration pollInterval) {
                Durations.requireAtLeastOneMillisecond(pollInterval, "pollInterval");
                this.pollInterval = pollInterval;
                return this;
            }

            public LeaderElectionBuilder withQuietPeriod(Duration quietPeriod) {
                Durations.requireAtLeastOneMillisecond(quietPeriod, "quietPeriod");
                this.quietPeriod = quietPeriod;
                return this;
            }

            public LeaderElectionBuilder withInstanceId(String instanceId) {
                Objects.requireNonNull(instanceId, "instanceId must not be null");
                if (instanceId.isEmpty()) {
                    throw new IllegalArgumentException("instanceId must not be empty");
                }
                this.instanceId = instanceId;
                return this;
            }

            public LeaderElectionBuilder onElected(Runnable callback) {
                Objects.requireNonNull(callback, "callback must not be null");
                this.onElected = token -> callback.run();
                return this;
            }

            public LeaderElectionBuilder onElected(Consumer<FencingToken> callback) {
                this.onElected = Objects.requireNonNull(callback, "callback must not be null");
                return this;
            }

            public LeaderElectionBuilder onRevoked(Runnable callback) {
                this.onRevoked = Objects.requireNonNull(callback, "callback must not be null");
                return this;
            }

            public LeaderElectionBuilder onCallbackError(Consumer<Throwable> handler) {
                this.onCallbackError = Objects.requireNonNull(handler, "handler must not be null");
                return this;
            }

            public LeaderElectionBuilder schemaMode(SchemaMode schemaMode) {
                this.schemaMode = Objects.requireNonNull(schemaMode, "schemaMode must not be null");
                return this;
            }

            public LeaderElection build() {
                schemaMode.applyToLocks(dataSource, tableName);
                Duration effectiveRenew = renewInterval != null ? renewInterval : leaseDuration.dividedBy(3);
                Duration effectivePoll = pollInterval != null ? pollInterval : leaseDuration.dividedBy(2);
                if (effectiveRenew.isZero()) {
                    throw new IllegalArgumentException("Default renewInterval (leaseDuration/3) is zero - leaseDuration is too small");
                }
                if (effectivePoll.isZero()) {
                    throw new IllegalArgumentException("Default pollInterval (leaseDuration/2) is zero - leaseDuration is too small");
                }
                Durations.requireAtLeastOneMillisecond(effectiveRenew, "renewInterval");
                Durations.requireAtLeastOneMillisecond(effectivePoll, "pollInterval");
                return new LeaderElectionInstance(
                    electionName,
                    dataSource,
                    tableName,
                    leaseDuration,
                    effectiveRenew,
                    effectivePoll,
                    quietPeriod,
                    instanceId,
                    onElected,
                    onRevoked,
                    onCallbackError);
            }
        }
    }

    public static final class Queues {

        private Queues() {
        }

        public static PublisherBuilder publisher(DataSource dataSource) {
            return new PublisherBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
        }

        public static ConsumerBuilder consumer(DataSource dataSource, String queueName) {
            Objects.requireNonNull(dataSource, "dataSource must not be null");
            Objects.requireNonNull(queueName, "queueName must not be null");
            if (queueName.isEmpty()) {
                throw new IllegalArgumentException("queueName must not be empty");
            }
            return new ConsumerBuilder(dataSource, queueName);
        }

        public static QueueBuilder queue(DataSource dataSource) {
            return new QueueBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
        }

        public static final class PublisherBuilder {
            private final DataSource dataSource;
            private String tableName = "fencepost_queue";
            private SchemaMode schemaMode = SchemaMode.NONE;

            private PublisherBuilder(DataSource dataSource) {
                this.dataSource = dataSource;
            }

            public PublisherBuilder tableName(String tableName) {
                validateTableName(tableName);
                this.tableName = tableName;
                return this;
            }

            public PublisherBuilder schemaMode(SchemaMode schemaMode) {
                this.schemaMode = Objects.requireNonNull(schemaMode, "schemaMode must not be null");
                return this;
            }

            public QueueFactory<QueuePublisher> build() {
                schemaMode.applyToQueue(dataSource, tableName);
                String t = this.tableName;
                return new QueueFactory<>(queueName -> new FencepostQueuePublisher(queueName, dataSource, t));
            }
        }

        public static final class ConsumerBuilder {
            private final DataSource dataSource;
            private final String queueName;
            private String tableName = "fencepost_queue";
            private Duration visibilityTimeout;
            private Duration pollInterval;
            private ThrowingConsumer<Message> handler;
            private int concurrency = 1;
            private BiConsumer<Message, Throwable> onError = (msg, t) -> {};
            private SchemaMode schemaMode = SchemaMode.NONE;

            private ConsumerBuilder(DataSource dataSource, String queueName) {
                this.dataSource = dataSource;
                this.queueName = queueName;
            }

            public ConsumerBuilder tableName(String tableName) {
                validateTableName(tableName);
                this.tableName = tableName;
                return this;
            }

            public ConsumerBuilder visibilityTimeout(Duration visibilityTimeout) {
                Durations.requireAtLeastOneMillisecond(visibilityTimeout, "visibilityTimeout");
                this.visibilityTimeout = visibilityTimeout;
                return this;
            }

            public ConsumerBuilder pollInterval(Duration pollInterval) {
                Durations.requireAtLeastOneMillisecond(pollInterval, "pollInterval");
                this.pollInterval = pollInterval;
                return this;
            }

            public ConsumerBuilder handler(ThrowingConsumer<Message> handler) {
                this.handler = Objects.requireNonNull(handler, "handler must not be null");
                return this;
            }

            public ConsumerBuilder concurrency(int concurrency) {
                if (concurrency < 1) {
                    throw new IllegalArgumentException("concurrency must be at least 1");
                }
                this.concurrency = concurrency;
                return this;
            }

            public ConsumerBuilder onError(BiConsumer<Message, Throwable> handler) {
                this.onError = Objects.requireNonNull(handler, "handler must not be null");
                return this;
            }

            public ConsumerBuilder schemaMode(SchemaMode schemaMode) {
                this.schemaMode = Objects.requireNonNull(schemaMode, "schemaMode must not be null");
                return this;
            }

            public QueueConsumer build() {
                schemaMode.applyToQueue(dataSource, tableName);
                if (visibilityTimeout == null) {
                    throw new IllegalStateException("visibilityTimeout must be set");
                }
                Objects.requireNonNull(handler, "handler must be set");
                QueueBuilder qb = Queues.queue(dataSource).tableName(tableName).visibilityTimeout(visibilityTimeout);
                if (pollInterval != null) {
                    qb.pollInterval(pollInterval);
                }
                Queue queue = qb.build().forName(queueName);
                return new QueueConsumerInstance(queueName, queue, handler, concurrency, onError);
            }
        }

        public static final class QueueBuilder {
            private final DataSource dataSource;
            private String tableName = "fencepost_queue";
            private Duration visibilityTimeout;
            private long pollIntervalMs = 100;
            private SchemaMode schemaMode = SchemaMode.NONE;

            private QueueBuilder(DataSource dataSource) {
                this.dataSource = dataSource;
            }

            public QueueBuilder tableName(String tableName) {
                validateTableName(tableName);
                this.tableName = tableName;
                return this;
            }

            public QueueBuilder visibilityTimeout(Duration visibilityTimeout) {
                Durations.requireAtLeastOneMillisecond(visibilityTimeout, "visibilityTimeout");
                this.visibilityTimeout = visibilityTimeout;
                return this;
            }

            public QueueBuilder pollInterval(Duration pollInterval) {
                this.pollIntervalMs = Durations.toPositiveMillis(pollInterval, "pollInterval");
                return this;
            }

            public QueueBuilder schemaMode(SchemaMode schemaMode) {
                this.schemaMode = Objects.requireNonNull(schemaMode, "schemaMode must not be null");
                return this;
            }

            public QueueFactory<Queue> build() {
                schemaMode.applyToQueue(dataSource, tableName);
                if (visibilityTimeout == null) {
                    throw new IllegalStateException("visibilityTimeout must be set");
                }
                String t = this.tableName;
                Duration vt = this.visibilityTimeout;
                long pi = this.pollIntervalMs;
                return new QueueFactory<>(queueName -> new FencepostQueue(queueName, dataSource, t, vt, pi));
            }
        }
    }
}
