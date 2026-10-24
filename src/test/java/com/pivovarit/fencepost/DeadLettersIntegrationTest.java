package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.DeadLetters;
import com.pivovarit.fencepost.queue.DeadMessage;
import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class DeadLettersIntegrationTest {

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
        TestSchema.resetQueue(dataSource);
    }

    private static DeadLetters deadLetters(String queueName) {
        return Fencepost.Queues.deadLetters(dataSource).build().forName(queueName);
    }

    private static Queue queue(String queueName) {
        return Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName(queueName);
    }

    /** Marks every live row of the queue dead; dead_at ascends with id so ordering is deterministic. */
    private static void markDead(String queueName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
               UPDATE fencepost_queue
               SET dead_at = timestamp with time zone '2026-01-01 00:00:00+00' + id * interval '1 second',
                   last_error = 'boom-' || id, attempts = 3
               WHERE queue_name = ? AND dead_at IS NULL""")) {
            ps.setString(1, queueName);
            ps.executeUpdate();
        }
    }

    /** Ids of all rows of the queue (any state), ascending. */
    private static List<Long> ids(String queueName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
               "SELECT id FROM fencepost_queue WHERE queue_name = ? ORDER BY id")) {
            ps.setString(1, queueName);
            List<Long> ids = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
            return ids;
        }
    }

    @Test
    void listReturnsAllFieldsOldestFirst() throws Exception {
        Queue q = queue("dl-list");
        q.enqueue("first".getBytes(UTF_8), "order", Map.of("k", "v"));
        q.enqueue("second".getBytes(UTF_8), "order", Map.of());
        markDead("dl-list");
        List<Long> ids = ids("dl-list");

        List<DeadMessage> dead = deadLetters("dl-list").list(10);

        assertThat(dead).hasSize(2);
        DeadMessage first = dead.get(0);
        assertThat(first.id()).isEqualTo(ids.get(0));
        assertThat(new String(first.payload(), UTF_8)).isEqualTo("first");
        assertThat(first.type()).contains("order");
        assertThat(first.headers()).containsExactly(Map.entry("k", "v"));
        assertThat(first.attempts()).isEqualTo(3);
        assertThat(first.lastError()).contains("boom-" + ids.get(0));
        assertThat(first.deadAt()).isNotNull();
        assertThat(dead.get(1).id()).isEqualTo(ids.get(1));
        assertThat(dead.get(1).deadAt()).isAfter(first.deadAt());
    }

    @Test
    void listRespectsLimit() throws Exception {
        Queue q = queue("dl-limit");
        q.enqueue("a".getBytes(UTF_8), "t", Map.of());
        q.enqueue("b".getBytes(UTF_8), "t", Map.of());
        q.enqueue("c".getBytes(UTF_8), "t", Map.of());
        markDead("dl-limit");

        assertThat(deadLetters("dl-limit").list(2)).extracting(DeadMessage::id)
          .containsExactlyElementsOf(ids("dl-limit").subList(0, 2));
    }

    @Test
    void listIgnoresLiveMessagesAndOtherQueues() throws Exception {
        queue("dl-live").enqueue("live".getBytes(UTF_8), "t", Map.of());
        queue("dl-other").enqueue("dead".getBytes(UTF_8), "t", Map.of());
        markDead("dl-other");

        assertThat(deadLetters("dl-live").list(10)).isEmpty();
        assertThat(deadLetters("dl-live").count()).isZero();
        assertThat(deadLetters("dl-other").count()).isEqualTo(1);
    }

    @Test
    void listHandlesRowsWithNullTypeHeadersAndLastError() throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
               INSERT INTO fencepost_queue (queue_name, payload, dead_at)
               VALUES ('dl-nulls', ?, now())""")) {
            ps.setBytes(1, "x".getBytes(UTF_8));
            ps.executeUpdate();
        }

        List<DeadMessage> dead = deadLetters("dl-nulls").list(10);

        assertThat(dead).hasSize(1);
        assertThat(dead.get(0).type()).isEqualTo(Optional.empty());
        assertThat(dead.get(0).headers()).isEmpty();
        assertThat(dead.get(0).lastError()).isEqualTo(Optional.empty());
    }

    @Test
    void worksAgainstACustomTable() throws Exception {
        DeadLetters custom = Fencepost.Queues.deadLetters(dataSource)
          .tableName("custom_dead_letters")
          .schemaMode(SchemaMode.CREATE)
          .build()
          .forName("dl-custom");

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute("""
              INSERT INTO custom_dead_letters (queue_name, payload, dead_at) VALUES ('dl-custom', 'x', now())""");
        }

        assertThat(custom.count()).isEqualTo(1);
        assertThat(custom.list(5)).hasSize(1);
    }

    @Test
    void failuresAreWrappedAndNameTheQueue() {
        DeadLetters missing = Fencepost.Queues.deadLetters(dataSource)
          .tableName("no_such_table")
          .build()
          .forName("dl-missing");

        assertThatThrownBy(() -> missing.count())
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("dl-missing");
        assertThatThrownBy(() -> missing.list(1))
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("dl-missing");
    }

    @Test
    void redriveMakesTheMessageConsumableWithAFreshDeliveryBudget() throws Exception {
        Queue q = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .maxDeliveries(3)
          .build()
          .forName("dl-redrive");
        q.enqueue("poison".getBytes(UTF_8), "t", Map.of());
        markDead("dl-redrive"); // attempts = 3 = maxDeliveries
        long id = ids("dl-redrive").get(0);
        DeadLetters dlq = deadLetters("dl-redrive");
        assertThat(q.tryDequeue()).isEmpty();

        assertThat(dlq.redrive(id)).isTrue();

        assertThat(dlq.count()).isZero();
        Message m = q.tryDequeue().orElseThrow();
        assertThat(m.id()).isEqualTo(id);
        assertThat(m.attempts()).as("budget reset: first delivery after redrive").isEqualTo(1);
        m.ack();
    }

    @Test
    void redriveClearsLastErrorAndDeadMarker() throws Exception {
        queue("dl-clear").enqueue("x".getBytes(UTF_8), "t", Map.of());
        markDead("dl-clear");
        long id = ids("dl-clear").get(0);

        assertThat(deadLetters("dl-clear").redrive(id)).isTrue();

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT dead_at, last_error, attempts, picked_by FROM fencepost_queue WHERE id = " + id)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject(1)).isNull();
            assertThat(rs.getObject(2)).isNull();
            assertThat(rs.getInt(3)).isZero();
            assertThat(rs.getObject(4)).isNull();
        }
    }

    @Test
    void redriveReturnsFalseForLiveUnknownAndForeignIds() throws Exception {
        queue("dl-false").enqueue("live".getBytes(UTF_8), "t", Map.of());
        long liveId = ids("dl-false").get(0);
        queue("dl-foreign").enqueue("dead".getBytes(UTF_8), "t", Map.of());
        markDead("dl-foreign");
        long foreignId = ids("dl-foreign").get(0);
        DeadLetters dlq = deadLetters("dl-false");

        assertThat(dlq.redrive(liveId)).as("live message").isFalse();
        assertThat(dlq.redrive(Long.MAX_VALUE)).as("unknown id").isFalse();
        assertThat(dlq.redrive(foreignId)).as("dead message of another queue").isFalse();
        assertThat(deadLetters("dl-foreign").count()).as("foreign message untouched").isEqualTo(1);
    }

    @Test
    void redriveAllRespectsTheCapAndRedrivesOldestFirst() throws Exception {
        Queue q = queue("dl-cap");
        for (String p : List.of("m1", "m2", "m3", "m4", "m5")) {
            q.enqueue(p.getBytes(UTF_8), "t", Map.of());
        }
        markDead("dl-cap");
        DeadLetters dlq = deadLetters("dl-cap");

        assertThat(dlq.redriveAll(2)).isEqualTo(2);
        assertThat(dlq.count()).isEqualTo(3);
        assertThat(new String(q.tryDequeue().orElseThrow().payload(), UTF_8)).isEqualTo("m1");
        assertThat(new String(q.tryDequeue().orElseThrow().payload(), UTF_8)).isEqualTo("m2");
        assertThat(q.tryDequeue()).isEmpty();

        assertThat(dlq.redriveAll(Integer.MAX_VALUE)).as("huge cap redrives the rest").isEqualTo(3);
        assertThat(dlq.redriveAll(10)).as("nothing left").isZero();
        assertThat(dlq.count()).isZero();
    }

    @Test
    void redriveWakesAConsumerBlockedInDequeue() throws Exception {
        Queue q = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .pollInterval(Duration.ofSeconds(30))
          .build()
          .forName("dl-wake");
        queue("dl-wake").enqueue("x".getBytes(UTF_8), "t", Map.of());
        markDead("dl-wake");
        long id = ids("dl-wake").get(0);

        CompletableFuture<Message> blocked = CompletableFuture.supplyAsync(() -> q.dequeue(Duration.ofSeconds(20)));
        Thread.sleep(500); // let the consumer reach its LISTEN wait
        long start = System.nanoTime();
        assertThat(deadLetters("dl-wake").redrive(id)).isTrue();

        Message m = blocked.get(10, TimeUnit.SECONDS);
        assertThat(m.id()).isEqualTo(id);
        assertThat(Duration.ofNanos(System.nanoTime() - start))
          .as("woken by NOTIFY, not by the 30s poll interval")
          .isLessThan(Duration.ofSeconds(10));
        m.ack();
        q.close();
    }

    @Test
    void concurrentRedriveAllNeverRedrivesARowTwice() throws Exception {
        Queue q = queue("dl-concurrent");
        for (int i = 0; i < 100; i++) {
            q.enqueue(("m" + i).getBytes(UTF_8), "t", Map.of());
        }
        markDead("dl-concurrent");
        DeadLetters dlq = deadLetters("dl-concurrent");
        int threads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return dlq.redriveAll(100);
                }));
            }
            start.countDown();
            int total = 0;
            for (Future<Integer> f : results) {
                total += f.get(30, TimeUnit.SECONDS);
            }
            assertThat(total).isEqualTo(100);
            assertThat(dlq.count()).isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void purgeDeletesOnlyTheDeadMessage() throws Exception {
        Queue q = queue("dl-purge");
        q.enqueue("dead".getBytes(UTF_8), "t", Map.of());
        markDead("dl-purge");
        q.enqueue("live".getBytes(UTF_8), "t", Map.of());
        List<Long> ids = ids("dl-purge");
        DeadLetters dlq = deadLetters("dl-purge");

        assertThat(dlq.purge(ids.get(0))).isTrue();
        assertThat(dlq.purge(ids.get(0))).as("already gone").isFalse();
        assertThat(dlq.purge(ids.get(1))).as("live message must not be deleted").isFalse();

        assertThat(ids("dl-purge")).containsExactly(ids.get(1));
        assertThat(new String(q.tryDequeue().orElseThrow().payload(), UTF_8)).isEqualTo("live");
    }

    @Test
    void purgeIgnoresDeadMessagesOfOtherQueues() throws Exception {
        queue("dl-purge-foreign").enqueue("dead".getBytes(UTF_8), "t", Map.of());
        markDead("dl-purge-foreign");
        long foreignId = ids("dl-purge-foreign").get(0);

        assertThat(deadLetters("dl-purge-mine").purge(foreignId)).isFalse();
        assertThat(deadLetters("dl-purge-mine").purgeAll(10)).isZero();

        assertThat(deadLetters("dl-purge-foreign").count()).isEqualTo(1);
    }

    @Test
    void purgeAllRespectsTheCapOldestFirstAndSparesLiveMessages() throws Exception {
        Queue q = queue("dl-purge-all");
        for (String p : List.of("d1", "d2", "d3")) {
            q.enqueue(p.getBytes(UTF_8), "t", Map.of());
        }
        markDead("dl-purge-all");
        q.enqueue("live".getBytes(UTF_8), "t", Map.of());
        List<Long> ids = ids("dl-purge-all");
        DeadLetters dlq = deadLetters("dl-purge-all");

        assertThat(dlq.purgeAll(2)).isEqualTo(2);
        assertThat(dlq.list(10)).extracting(DeadMessage::id).containsExactly(ids.get(2));

        assertThat(dlq.purgeAll(Integer.MAX_VALUE)).isEqualTo(1);
        assertThat(dlq.purgeAll(10)).isZero();
        assertThat(ids("dl-purge-all")).as("live message survives").containsExactly(ids.get(3));
    }
}
