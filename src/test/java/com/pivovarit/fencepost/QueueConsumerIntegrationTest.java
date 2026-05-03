package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Message;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class QueueConsumerIntegrationTest {

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
            conn.createStatement().execute(
              "DROP TABLE IF EXISTS fencepost_queue; "
                + "CREATE TABLE fencepost_queue ("
                + "  id BIGSERIAL PRIMARY KEY,"
                + "  queue_name TEXT NOT NULL,"
                + "  payload BYTEA NOT NULL,"
                + "  type TEXT,"
                + "  headers JSONB,"
                + "  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "  visible_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "  attempts INT NOT NULL DEFAULT 0,"
                + "  picked_by TEXT"
                + ");"
                + "CREATE INDEX idx_fencepost_queue_dequeue ON fencepost_queue (queue_name, visible_at)"
            );
        }
    }

    @Test
    void shouldConsumeMessages() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> {
                received.add(new String(msg.payload(), UTF_8));
                latch.countDown();
            })
            .build();

        enqueue("msg-1", "msg-2", "msg-3");
        consumer.start();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactlyInAnyOrder("msg-1", "msg-2", "msg-3");

        consumer.close();
    }

    @Test
    void shouldAutoAckOnSuccess() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> latch.countDown())
            .build();

        enqueue("auto-ack");
        consumer.start();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);

        assertThat(queueSize()).isZero();
        consumer.close();
    }

    @Test
    void shouldAutoNackOnHandlerException() throws Exception {
        CountDownLatch firstAttempt = new CountDownLatch(1);
        CountDownLatch secondAttempt = new CountDownLatch(2);
        List<Integer> attempts = new CopyOnWriteArrayList<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> {
                attempts.add(msg.attempts());
                firstAttempt.countDown();
                secondAttempt.countDown();
                if (msg.attempts() == 1) {
                    throw new RuntimeException("simulated failure");
                }
            })
            .onError((msg, t) -> {})
            .build();

        enqueue("will-fail-once");
        consumer.start();

        assertThat(secondAttempt.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(attempts).containsExactly(1, 2);

        consumer.close();
    }

    @Test
    void shouldCallOnErrorWhenHandlerThrows() throws Exception {
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        AtomicReference<Message> capturedMsg = new AtomicReference<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> { throw new IllegalArgumentException("bad payload"); })
            .onError((msg, t) -> {
                capturedMsg.set(msg);
                capturedError.set(t);
                errorLatch.countDown();
            })
            .build();

        enqueue("bad-msg");
        consumer.start();

        assertThat(errorLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedError.get()).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("bad payload");
        assertThat(capturedMsg.get()).isNotNull();
        assertThat(capturedMsg.get().payload()).isEqualTo("bad-msg".getBytes(UTF_8));

        consumer.close();
    }

    @Test
    void shouldSupportConcurrentConsumers() throws Exception {
        int messageCount = 20;
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(messageCount);
        List<String> threadNames = new CopyOnWriteArrayList<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> {
                threadNames.add(Thread.currentThread().getName());
                received.add(new String(msg.payload(), UTF_8));
                latch.countDown();
            })
            .concurrency(3)
            .build();

        for (int i = 0; i < messageCount; i++) {
            enqueue("concurrent-" + i);
        }
        consumer.start();

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received).hasSize(messageCount).doesNotHaveDuplicates();

        consumer.close();
    }

    @Test
    void shouldBlockUntilMessageArrives() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> latch.countDown())
            .build();

        consumer.start();
        Thread.sleep(500);
        assertThat(latch.getCount()).isEqualTo(1);

        enqueue("late-arrival");

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        consumer.close();
    }

    @Test
    void closeShouldStopConsumption() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        List<String> received = new CopyOnWriteArrayList<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> {
                received.add(new String(msg.payload(), UTF_8));
                started.countDown();
            })
            .build();

        enqueue("before-close");
        consumer.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        consumer.close();

        enqueue("after-close");
        Thread.sleep(500);

        assertThat(received).containsExactly("before-close");
    }

    @Test
    void startAfterCloseShouldThrow() {
        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> {})
            .build();

        consumer.close();

        assertThatThrownBy(consumer::start)
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void startShouldBeIdempotent() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> latch.countDown())
            .build();

        consumer.start();
        consumer.start();

        enqueue("idempotent");
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        consumer.close();
    }

    @Test
    void closeShouldBeIdempotent() {
        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .handler(msg -> {})
            .build();

        consumer.start();
        consumer.close();
        consumer.close();
    }

    @Test
    void builderShouldRequireHandler() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(dataSource, "test-queue")
            .visibilityTimeout(Duration.ofMinutes(5))
            .build())
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void builderShouldRequireVisibilityTimeout() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(dataSource, "test-queue")
            .handler(msg -> {})
            .build())
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void builderShouldRejectZeroConcurrency() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(dataSource, "test-queue")
            .concurrency(0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builderShouldRejectNullQueueName() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(dataSource, null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void builderShouldRejectEmptyQueueName() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(dataSource, ""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private void enqueue(String... messages) {
        var queue = Fencepost.Queues.queue(dataSource)
            .visibilityTimeout(Duration.ofMinutes(5))
            .build()
            .forName("test-queue");
        for (String msg : messages) {
            queue.enqueue(msg.getBytes(UTF_8));
        }
    }

    private long queueSize() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
                 "SELECT COUNT(*) FROM fencepost_queue WHERE queue_name = 'test-queue'")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
