package com.pivovarit.fencepost;

import com.pivovarit.fencepost.election.LeaderElection;
import com.pivovarit.fencepost.lock.FencingToken;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class LeaderElectionIntegrationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource dataSource;

    @BeforeAll
    static void setupDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;
    }

    @BeforeEach
    void createTable() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute("DROP TABLE IF EXISTS fencepost_locks; CREATE TABLE fencepost_locks (  lock_name TEXT PRIMARY KEY,  token BIGINT NOT NULL DEFAULT 0,  locked_by TEXT,  locked_at TIMESTAMP WITH TIME ZONE,  expires_at TIMESTAMP WITH TIME ZONE)");
        }
    }

    @Test
    void singleInstanceBecomesLeader() {
        AtomicReference<FencingToken> seenToken = new AtomicReference<>();
        try (LeaderElection election = Fencepost.leaderElection(dataSource, "single", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .onElected((Consumer<FencingToken>) seenToken::set)
            .build()) {
            election.start();
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(election.isLeader()).isTrue();
                assertThat(seenToken.get()).isNotNull();
                assertThat(seenToken.get().value()).isGreaterThan(0);
            });
        }
    }

    @Test
    void exactlyOneOfTwoBecomesLeader() {
        AtomicInteger electedCount = new AtomicInteger();
        try (LeaderElection a = Fencepost.leaderElection(dataSource, "two-way", Duration.ofSeconds(5))
                .withRenewInterval(Duration.ofMillis(500))
                .withPollInterval(Duration.ofMillis(100))
                .withInstanceId("a")
                .onElected(electedCount::incrementAndGet)
                .build();
             LeaderElection b = Fencepost.leaderElection(dataSource, "two-way", Duration.ofSeconds(5))
                .withRenewInterval(Duration.ofMillis(500))
                .withPollInterval(Duration.ofMillis(100))
                .withInstanceId("b")
                .onElected(electedCount::incrementAndGet)
                .build()) {
            a.start();
            b.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> a.isLeader() || b.isLeader());
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            assertThat(a.isLeader() ^ b.isLeader()).isTrue();
            assertThat(electedCount.get()).isEqualTo(1);
        }
    }

    @Test
    void failoverOnGracefulClose() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch aIsLeader = new CountDownLatch(1);

        LeaderElection a = Fencepost.leaderElection(dataSource, "graceful-failover", Duration.ofSeconds(3))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withInstanceId("a")
            .onElected(() -> { events.add("a-elected"); aIsLeader.countDown(); })
            .onRevoked(() -> events.add("a-revoked"))
            .build();
        LeaderElection b = Fencepost.leaderElection(dataSource, "graceful-failover", Duration.ofSeconds(3))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withInstanceId("b")
            .onElected(() -> events.add("b-elected"))
            .onRevoked(() -> events.add("b-revoked"))
            .build();

        try {
            a.start();
            assertThat(aIsLeader.await(5, TimeUnit.SECONDS)).isTrue();
            b.start();

            a.close();
            assertThat(events).contains("a-revoked");

            await().atMost(5, TimeUnit.SECONDS).until(b::isLeader);
            assertThat(events).contains("b-elected");
            assertThat(events.indexOf("a-revoked")).isLessThan(events.indexOf("b-elected"));
        } finally {
            b.close();
        }
    }

    @Test
    void standbyAcquiresAfterLeaseRowCleared() throws Exception {
        CountDownLatch aIsLeader = new CountDownLatch(1);
        CountDownLatch bIsLeader = new CountDownLatch(1);

        try (LeaderElection a = Fencepost.leaderElection(dataSource, "ttl-failover", Duration.ofSeconds(10))
                .withRenewInterval(Duration.ofSeconds(2))
                .withPollInterval(Duration.ofMillis(200))
                .withInstanceId("a")
                .onElected(aIsLeader::countDown)
                .build();
             LeaderElection b = Fencepost.leaderElection(dataSource, "ttl-failover", Duration.ofSeconds(10))
                .withRenewInterval(Duration.ofSeconds(2))
                .withPollInterval(Duration.ofMillis(200))
                .withInstanceId("b")
                .onElected(bIsLeader::countDown)
                .build()) {
            a.start();
            assertThat(aIsLeader.await(5, TimeUnit.SECONDS)).isTrue();
            b.start();

            try (Connection conn = dataSource.getConnection()) {
                conn.createStatement().execute(
                    "UPDATE fencepost_locks SET locked_by = NULL, locked_at = NULL, expires_at = NULL WHERE lock_name = 'ttl-failover'");
            }

            assertThat(bIsLeader.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void revokeFiresOnAutoRenewFailureAndReacquires() throws Exception {
        CountDownLatch firstElect = new CountDownLatch(1);
        CountDownLatch revoked = new CountDownLatch(1);
        AtomicInteger electedCount = new AtomicInteger();

        try (LeaderElection election = Fencepost.leaderElection(dataSource, "renew-fail", Duration.ofSeconds(3))
            .withRenewInterval(Duration.ofMillis(300))
            .withPollInterval(Duration.ofMillis(200))
            .onElected(() -> { electedCount.incrementAndGet(); firstElect.countDown(); })
            .onRevoked(revoked::countDown)
            .build()) {
            election.start();
            assertThat(firstElect.await(5, TimeUnit.SECONDS)).isTrue();

            try (Connection conn = dataSource.getConnection()) {
                conn.createStatement().execute("DROP TABLE fencepost_locks");
            }

            assertThat(revoked.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(election.isLeader()).isFalse();

            try (Connection conn = dataSource.getConnection()) {
                conn.createStatement().execute("CREATE TABLE fencepost_locks (  lock_name TEXT PRIMARY KEY,  token BIGINT NOT NULL DEFAULT 0,  locked_by TEXT,  locked_at TIMESTAMP WITH TIME ZONE,  expires_at TIMESTAMP WITH TIME ZONE)");
            }
            await().atMost(10, TimeUnit.SECONDS).until(election::isLeader);
            assertThat(electedCount.get()).isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void callbackExceptionGoesToErrorHandlerAndLoopContinues() throws Exception {
        CountDownLatch electedFired = new CountDownLatch(1);
        AtomicReference<Throwable> seenError = new AtomicReference<>();

        try (LeaderElection election = Fencepost.leaderElection(dataSource, "boom", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .onElected(() -> { electedFired.countDown(); throw new RuntimeException("boom"); })
            .onCallbackError(seenError::set)
            .build()) {
            election.start();
            assertThat(electedFired.await(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(2, TimeUnit.SECONDS).until(() -> seenError.get() != null);
            assertThat(seenError.get()).hasMessage("boom");
            assertThat(election.isLeader()).isTrue();
        }
    }

    @Test
    void closeWhileStandbyDoesNotFireOnRevoked() throws Exception {
        AtomicInteger revokedCount = new AtomicInteger();
        CountDownLatch aIsLeader = new CountDownLatch(1);

        LeaderElection a = Fencepost.leaderElection(dataSource, "standby-close", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withInstanceId("a")
            .onElected(aIsLeader::countDown)
            .build();
        LeaderElection b = Fencepost.leaderElection(dataSource, "standby-close", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withInstanceId("b")
            .onRevoked(revokedCount::incrementAndGet)
            .build();

        try {
            a.start();
            assertThat(aIsLeader.await(5, TimeUnit.SECONDS)).isTrue();

            b.start();
            Thread.sleep(500);
            assertThat(b.isLeader()).isFalse();

            b.close();
            assertThat(revokedCount.get()).isZero();
        } finally {
            a.close();
        }
    }

    @Test
    void startAndCloseAreIdempotent() throws Exception {
        AtomicInteger electedCount = new AtomicInteger();
        AtomicInteger revokedCount = new AtomicInteger();
        CountDownLatch elected = new CountDownLatch(1);

        LeaderElection election = Fencepost.leaderElection(dataSource, "idempotent", Duration.ofSeconds(3))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .onElected(() -> { electedCount.incrementAndGet(); elected.countDown(); })
            .onRevoked(revokedCount::incrementAndGet)
            .build();

        election.start();
        election.start();
        assertThat(elected.await(5, TimeUnit.SECONDS)).isTrue();

        election.close();
        election.close();

        assertThat(electedCount.get()).isEqualTo(1);
        assertThat(revokedCount.get()).isEqualTo(1);
    }

    @Test
    void fencingTokenIsStrictlyIncreasingAcrossReacquisitions() throws Exception {
        List<Long> tokens = new CopyOnWriteArrayList<>();

        try (LeaderElection a = Fencepost.leaderElection(dataSource, "tokens", Duration.ofSeconds(3))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withInstanceId("a")
            .onElected((Consumer<FencingToken>) t -> tokens.add(t.value()))
            .build()) {
            a.start();
            await().atMost(5, TimeUnit.SECONDS).until(a::isLeader);
            a.close();

            try (LeaderElection b = Fencepost.leaderElection(dataSource, "tokens", Duration.ofSeconds(3))
                .withRenewInterval(Duration.ofMillis(500))
                .withPollInterval(Duration.ofMillis(100))
                .withInstanceId("b")
                .onElected((Consumer<FencingToken>) t -> tokens.add(t.value()))
                .build()) {
                b.start();
                await().atMost(5, TimeUnit.SECONDS).until(b::isLeader);
                b.close();
            }
        }

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1)).isGreaterThan(tokens.get(0));
    }

    @Test
    void startAfterCloseThrowsIllegalStateException() {
        LeaderElection election = Fencepost.leaderElection(dataSource, "closed", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .build();

        election.close();

        assertThatThrownBy(election::start)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("closed");
    }

    @Test
    void quietPeriodGatesReacquisitionByOtherInstance() throws Exception {
        CountDownLatch aElected = new CountDownLatch(1);
        AtomicReference<Long> bElectedAt = new AtomicReference<>();

        LeaderElection a = Fencepost.leaderElection(dataSource, "quiet", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withQuietPeriod(Duration.ofSeconds(2))
            .withInstanceId("a")
            .onElected(aElected::countDown)
            .build();
        LeaderElection b = Fencepost.leaderElection(dataSource, "quiet", Duration.ofSeconds(5))
            .withRenewInterval(Duration.ofMillis(500))
            .withPollInterval(Duration.ofMillis(100))
            .withQuietPeriod(Duration.ofSeconds(2))
            .withInstanceId("b")
            .onElected(() -> bElectedAt.compareAndSet(null, System.nanoTime()))
            .build();

        try {
            a.start();
            assertThat(aElected.await(5, TimeUnit.SECONDS)).isTrue();
            b.start();

            long releaseAt = System.nanoTime();
            a.close();

            await().atMost(10, TimeUnit.SECONDS).until(() -> bElectedAt.get() != null);
            long gapMs = (bElectedAt.get() - releaseAt) / 1_000_000L;
            assertThat(gapMs).isGreaterThanOrEqualTo(1500);
        } finally {
            b.close();
        }
    }
}
