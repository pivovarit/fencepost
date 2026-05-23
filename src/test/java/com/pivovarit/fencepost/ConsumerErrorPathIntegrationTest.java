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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ConsumerErrorPathIntegrationTest {

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
    void onErrorHandlerExceptionShouldNotCrashConsumer() throws Exception {
        CountDownLatch firstProcessed = new CountDownLatch(1);
        CountDownLatch secondProcessed = new CountDownLatch(2);
        List<String> received = new CopyOnWriteArrayList<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "error-handler-crash")
          .visibilityTimeout(Duration.ofMinutes(5))
          .handler(msg -> {
              received.add(new String(msg.payload(), UTF_8));
              firstProcessed.countDown();
              secondProcessed.countDown();
          })
          .onError((msg, t) -> {
              throw new RuntimeException("onError handler blew up");
          })
          .build();

        enqueue("error-handler-crash", "msg-1", "msg-2");
        consumer.start();

        assertThat(secondProcessed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactlyInAnyOrder("msg-1", "msg-2");

        consumer.close();
    }

    @Test
    void handlerThrowsThenNackSucceedsShouldRedeliverMessage() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch secondAttempt = new CountDownLatch(1);

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "handler-nack-redeliver")
          .visibilityTimeout(Duration.ofMinutes(5))
          .handler(msg -> {
              int attempt = attempts.incrementAndGet();
              if (attempt == 1) {
                  throw new RuntimeException("first attempt fails");
              }
              secondAttempt.countDown();
          })
          .onError((msg, t) -> {})
          .build();

        enqueue("handler-nack-redeliver", "will-retry");
        consumer.start();

        assertThat(secondAttempt.await(10, TimeUnit.SECONDS))
          .as("message should be redelivered after handler failure + nack")
          .isTrue();
        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);

        consumer.close();
    }

    @Test
    void ackFailureShouldCallOnError() throws Exception {
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);

        Queue queue = Fencepost.Queues.queue(faultDs)
          .visibilityTimeout(Duration.ofSeconds(2))
          .build()
          .forName("ack-fail-error");

        queue.enqueue("ack-will-fail".getBytes(UTF_8), "test", Map.of());

        QueueConsumer consumer = Fencepost.Queues.consumer(faultDs, "ack-fail-error")
          .visibilityTimeout(Duration.ofSeconds(2))
          .handler(msg -> {
              faultDs.failNext(1);
          })
          .onError((msg, t) -> {
              if (msg != null) {
                  capturedError.set(t);
                  errorLatch.countDown();
              }
          })
          .build();

        consumer.start();

        assertThat(errorLatch.await(10, TimeUnit.SECONDS))
          .as("onError should be called when ack fails after successful handler")
          .isTrue();
        assertThat(capturedError.get()).isNotNull();

        consumer.close();
    }

    @Test
    void consumerShouldRecoverFromDequeueFailure() throws Exception {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> payload = new AtomicReference<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(faultDs, "dequeue-recovery")
          .visibilityTimeout(Duration.ofMinutes(5))
          .handler(msg -> {
              payload.set(new String(msg.payload(), UTF_8));
              received.countDown();
          })
          .onError((msg, t) -> {})
          .build();

        faultDs.failNext(3);
        enqueue("dequeue-recovery", "after-recovery");
        consumer.start();

        assertThat(received.await(15, TimeUnit.SECONDS))
          .as("consumer should recover from transient dequeue failures and process the message")
          .isTrue();
        assertThat(payload.get()).isEqualTo("after-recovery");

        consumer.close();
    }

    @Test
    void messageWithTypeAndHeadersShouldSurviveConsumerRoundtrip() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Message> captured = new AtomicReference<>();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "roundtrip-meta")
          .visibilityTimeout(Duration.ofMinutes(5))
          .handler(msg -> {
              captured.set(msg);
              latch.countDown();
          })
          .build();

        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("roundtrip-meta");
        queue.enqueue("body".getBytes(UTF_8), "email.send", Map.of("key", "value"));

        consumer.start();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(captured.get().type()).contains("email.send");
        assertThat(captured.get().headers()).containsEntry("key", "value");

        consumer.close();
    }

    @Test
    void consumerShouldNotProcessAfterClose() throws Exception {
        CountDownLatch firstReceived = new CountDownLatch(1);
        AtomicInteger processedCount = new AtomicInteger();

        QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "no-process-after-close")
          .visibilityTimeout(Duration.ofMinutes(5))
          .handler(msg -> {
              processedCount.incrementAndGet();
              firstReceived.countDown();
          })
          .build();

        enqueue("no-process-after-close", "before");
        consumer.start();
        assertThat(firstReceived.await(5, TimeUnit.SECONDS)).isTrue();

        consumer.close();
        int countAtClose = processedCount.get();

        enqueue("no-process-after-close", "after-1", "after-2");
        Thread.sleep(500);

        assertThat(processedCount.get()).isEqualTo(countAtClose);
    }

    @Test
    void messageShouldBecomeAvailableAfterVisibilityTimeoutWhenNackFails() throws Exception {
        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofSeconds(1))
          .build()
          .forName("nack-fail-visibility");

        queue.enqueue("poison".getBytes(UTF_8), "test", Map.of());

        Message msg = queue.tryDequeue().get();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_queue SET visible_at = now() - interval '1 second' WHERE id = " + msg.id());
        }

        Message stolen = queue.tryDequeue().get();

        msg.close();

        Thread.sleep(1_500);

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT COUNT(*) FROM fencepost_queue WHERE queue_name = 'nack-fail-visibility'")) {
            rs.next();
            assertThat(rs.getInt(1))
              .as("message should still exist in queue for redelivery")
              .isEqualTo(1);
        }

        stolen.ack();
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
