package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import com.pivovarit.fencepost.queue.QueueConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class DlqIntegrationTest {

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

    @Test
    void deadRowsAreNotDequeued() throws Exception {
        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("dlq-skip");

        queue.enqueue("dead".getBytes(UTF_8), "test", Map.of());
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_queue SET dead_at = now() WHERE queue_name = 'dlq-skip'");
        }

        assertThat(queue.tryDequeue())
          .as("dead-lettered rows must not be dequeued")
          .isEmpty();

        queue.enqueue("live".getBytes(UTF_8), "test", Map.of());
        Message live = queue.tryDequeue().orElseThrow();
        assertThat(new String(live.payload(), UTF_8)).isEqualTo("live");
        live.ack();
    }

    @Test
    void nackWithDelayDelaysRedelivery() throws Exception {
        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("dlq-nack-delay");
        queue.enqueue("retry".getBytes(UTF_8), "test", Map.of());

        AckableMessage m = (AckableMessage) queue.tryDequeue().orElseThrow();
        m.nack(Duration.ofSeconds(2));

        assertThat(queue.tryDequeue())
          .as("message should still be delayed immediately after nack(2s)")
          .isEmpty();

        AtomicReference<Message> redelivered = new AtomicReference<>();
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            Optional<Message> picked = queue.tryDequeue();
            picked.ifPresent(redelivered::set);
            return picked.isPresent();
        });
        assertThat(new String(redelivered.get().payload(), UTF_8)).isEqualTo("retry");
        redelivered.get().ack();
    }

    @Test
    void deadLetterMarksRowAndStopsRedelivery() throws Exception {
        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("dlq-manual-dead");
        queue.enqueue("poison".getBytes(UTF_8), "test", Map.of());

        AckableMessage m = (AckableMessage) queue.tryDequeue().orElseThrow();
        m.deadLetter("boom");

        assertThat(queue.tryDequeue())
          .as("dead-lettered message must not come back")
          .isEmpty();

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT dead_at, last_error FROM fencepost_queue WHERE queue_name = 'dlq-manual-dead'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTimestamp("dead_at")).isNotNull();
            assertThat(rs.getString("last_error")).isEqualTo("boom");
        }
    }

    @Test
    void maxDeliveriesMustBeAtLeastOne() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(dataSource, "dlq-validate").maxDeliveries(0))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("maxDeliveries must be at least 1");
    }

    @Test
    void messageIsDeadLetteredAfterMaxDeliveries() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "dlq-max")
          .visibilityTimeout(Duration.ofMinutes(5))
          .maxDeliveries(3)
          .retryDelay(Duration.ofMillis(50))
          .handler(msg -> {
              deliveries.incrementAndGet();
              throw new RuntimeException("always fails");
          })
          .build();

        enqueue("dlq-max", "poison");
        consumer.start();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try (Connection conn = dataSource.getConnection();
                 ResultSet rs = conn.createStatement().executeQuery(
                   "SELECT dead_at, attempts, last_error FROM fencepost_queue WHERE queue_name = 'dlq-max'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getTimestamp("dead_at")).isNotNull();
                assertThat(rs.getInt("attempts")).isEqualTo(3);
                assertThat(rs.getString("last_error")).contains("always fails");
            }
        });

        Thread.sleep(500);
        assertThat(deliveries.get())
          .as("poison message should be delivered exactly maxDeliveries times")
          .isEqualTo(3);

        consumer.close();
    }

    @Test
    void retryDelaySpacesOutRedeliveries() throws Exception {
        List<Long> attemptTimes = new CopyOnWriteArrayList<>();
        CountDownLatch twice = new CountDownLatch(2);
        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "dlq-retry-delay")
          .visibilityTimeout(Duration.ofMinutes(5))
          .retryDelay(Duration.ofSeconds(2))
          .handler(msg -> {
              attemptTimes.add(System.nanoTime());
              twice.countDown();
              throw new RuntimeException("fail to force retry");
          })
          .build();

        enqueue("dlq-retry-delay", "msg");
        consumer.start();

        assertThat(twice.await(10, TimeUnit.SECONDS)).isTrue();
        consumer.close();

        long gapMs = TimeUnit.NANOSECONDS.toMillis(attemptTimes.get(1) - attemptTimes.get(0));
        assertThat(gapMs)
          .as("redelivery should respect retryDelay (2s)")
          .isGreaterThanOrEqualTo(1_500);
    }

    @Test
    void unlimitedDeliveriesByDefaultNeverDeadLetters() throws Exception {
        CountDownLatch threeAttempts = new CountDownLatch(3);
        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "dlq-unlimited")
          .visibilityTimeout(Duration.ofMinutes(5))
          .retryDelay(Duration.ofMillis(50))
          .handler(msg -> {
              threeAttempts.countDown();
              throw new RuntimeException("keep failing");
          })
          .build();

        enqueue("dlq-unlimited", "msg");
        consumer.start();

        assertThat(threeAttempts.await(10, TimeUnit.SECONDS)).isTrue();
        consumer.close();

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT dead_at FROM fencepost_queue WHERE queue_name = 'dlq-unlimited'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTimestamp("dead_at"))
              .as("no maxDeliveries set -> never dead-lettered")
              .isNull();
        }
    }

    @Test
    void onErrorReceivesExceptionOnDeadLetterDelivery() throws Exception {
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "dlq-onerror")
          .visibilityTimeout(Duration.ofMinutes(5))
          .maxDeliveries(2)
          .retryDelay(Duration.ofMillis(50))
          .handler(msg -> {
              throw new RuntimeException("boom-" + msg.attempts());
          })
          .onError((msg, t) -> errors.add(t))
          .build();

        enqueue("dlq-onerror", "poison");
        consumer.start();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try (Connection conn = dataSource.getConnection();
                 ResultSet rs = conn.createStatement().executeQuery(
                   "SELECT dead_at FROM fencepost_queue WHERE queue_name = 'dlq-onerror'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getTimestamp("dead_at")).isNotNull();
            }
        });
        consumer.close();

        assertThat(errors)
          .as("onError fires on every failed delivery, including the terminal dead-lettering one")
          .hasSize(2);
        assertThat(errors).allSatisfy(t -> assertThat(t).hasMessageStartingWith("boom-"));
        assertThat(errors.get(1)).hasMessage("boom-2");
    }

    private void enqueue(String queueName, String... messages) {
        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName(queueName);
        for (String msg : messages) {
            queue.enqueue(msg.getBytes(UTF_8), "test", Map.of());
        }
    }
}
