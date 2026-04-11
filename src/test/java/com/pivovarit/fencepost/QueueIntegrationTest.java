package com.pivovarit.fencepost;

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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class QueueIntegrationTest {

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
                + "  payload TEXT NOT NULL,"
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
    void shouldEnqueueAndTryDequeue() {
        Queue queue = newQueue();
        queue.enqueue("hello");

        Optional<Message> msg = queue.tryDequeue();

        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("hello");
        assertThat(msg.get().attempts()).isEqualTo(1);
        msg.get().ack();
    }

    @Test
    void tryDequeueShouldReturnEmptyWhenQueueIsEmpty() {
        Queue queue = newQueue();

        Optional<Message> msg = queue.tryDequeue();

        assertThat(msg).isEmpty();
    }

    @Test
    void shouldDequeueInFIFOOrder() {
        Queue queue = newQueue();
        queue.enqueue("first");
        queue.enqueue("second");
        queue.enqueue("third");

        assertThat(queue.tryDequeue().get().payload()).isEqualTo("first");
        assertThat(queue.tryDequeue().get().payload()).isEqualTo("second");
        assertThat(queue.tryDequeue().get().payload()).isEqualTo("third");
        assertThat(queue.tryDequeue()).isEmpty();
    }

    @Test
    void shouldRespectDelayedEnqueue() throws Exception {
        Queue queue = newQueue();
        queue.enqueue("delayed", Duration.ofSeconds(2));

        assertThat(queue.tryDequeue()).isEmpty();

        Thread.sleep(2_500);

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("delayed");
        msg.get().ack();
    }

    @Test
    void shouldIsolateNamedQueues() {
        Queue emails = newQueue("emails");
        Queue webhooks = newQueue("webhooks");

        emails.enqueue("email-1");
        webhooks.enqueue("webhook-1");

        assertThat(emails.tryDequeue().get().payload()).isEqualTo("email-1");
        assertThat(webhooks.tryDequeue().get().payload()).isEqualTo("webhook-1");
        assertThat(emails.tryDequeue()).isEmpty();
        assertThat(webhooks.tryDequeue()).isEmpty();
    }

    @Test
    void ackShouldDeleteMessage() {
        Queue queue = newQueue();
        queue.enqueue("to-ack");

        Message msg = queue.tryDequeue().get();
        msg.ack();

        assertThat(queue.tryDequeue()).isEmpty();
    }

    @Test
    void nackShouldMakeMessageImmediatelyAvailable() {
        Queue queue = newQueue();
        queue.enqueue("to-nack");

        Message msg = queue.tryDequeue().get();
        msg.nack();

        Optional<Message> redelivered = queue.tryDequeue();
        assertThat(redelivered).isPresent();
        assertThat(redelivered.get().payload()).isEqualTo("to-nack");
        assertThat(redelivered.get().attempts()).isEqualTo(2);
        redelivered.get().ack();
    }

    @Test
    void closeWithoutAckShouldLetVisibilityTimeoutExpire() {
        Queue shortTimeout = Fencepost.queue(dataSource).visibilityTimeout(Duration.ofSeconds(1)).build().forName("test-queue");
        shortTimeout.enqueue("to-expire");

        Message msg = shortTimeout.tryDequeue().get();
        msg.close();

        assertThat(shortTimeout.tryDequeue()).isEmpty();
    }

    @Test
    void ackAfterAckShouldThrow() {
        Queue queue = newQueue();
        queue.enqueue("double-ack");

        Message msg = queue.tryDequeue().get();
        msg.ack();

        assertThatThrownBy(msg::ack)
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void nackAfterAckShouldThrow() {
        Queue queue = newQueue();
        queue.enqueue("ack-then-nack");

        Message msg = queue.tryDequeue().get();
        msg.ack();

        assertThatThrownBy(msg::nack)
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void ackAfterNackShouldThrow() {
        Queue queue = newQueue();
        queue.enqueue("nack-then-ack");

        Message msg = queue.tryDequeue().get();
        msg.nack();

        assertThatThrownBy(msg::ack)
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void dequeueShouldBlockUntilMessageAvailable() throws Exception {
        Queue queue = newQueue();

        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Message> ref = new AtomicReference<>();

        Thread consumer = new Thread(() -> {
            ref.set(queue.dequeue());
            received.countDown();
        });
        consumer.start();

        Thread.sleep(200);
        assertThat(received.getCount()).isEqualTo(1);

        queue.enqueue("wake-up");

        assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ref.get().payload()).isEqualTo("wake-up");
        ref.get().ack();
        queue.close();
    }

    @Test
    void dequeueWithTimeoutShouldThrowOnTimeout() {
        Queue queue = newQueue();

        assertThatThrownBy(() -> queue.dequeue(Duration.ofMillis(500)))
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("timed out");
        queue.close();
    }

    @Test
    void dequeueWithTimeoutShouldReturnBeforeTimeout() {
        Queue queue = newQueue();
        queue.enqueue("already-here");

        Message msg = queue.dequeue(Duration.ofSeconds(5));
        assertThat(msg.payload()).isEqualTo("already-here");
        msg.ack();
        queue.close();
    }

    @Test
    void concurrentConsumersShouldNotReceiveSameMessage() throws Exception {
        Queue queue = newQueue();
        int messageCount = 50;
        for (int i = 0; i < messageCount; i++) {
            queue.enqueue("msg-" + i);
        }

        List<String> consumed = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(messageCount);

        Runnable consumer = () -> {
            while (true) {
                Optional<Message> msg = queue.tryDequeue();
                if (msg.isEmpty()) {
                    break;
                }
                consumed.add(msg.get().payload());
                msg.get().ack();
                done.countDown();
            }
        };

        Thread t1 = new Thread(consumer);
        Thread t2 = new Thread(consumer);
        Thread t3 = new Thread(consumer);
        t1.start();
        t2.start();
        t3.start();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumed).hasSize(messageCount);
        assertThat(consumed).doesNotHaveDuplicates();
    }

    @Test
    void shouldWorkWithBuilderAPI() {
        Factory<Queue> factory = Fencepost.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build();

        Queue queue = factory.forName("builder-test");
        queue.enqueue("via-builder");

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("via-builder");
        msg.get().ack();
    }

    @Test
    void builderShouldRequireVisibilityTimeout() {
        assertThatThrownBy(() -> Fencepost.queue(dataSource).build())
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void builderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.queue(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void builderShouldRejectInvalidVisibilityTimeout() {
        assertThatThrownBy(() -> Fencepost.queue(dataSource).visibilityTimeout(Duration.ZERO))
          .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> Fencepost.queue(dataSource).visibilityTimeout(Duration.ofSeconds(-1)))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void messageShouldBecomeVisibleAfterVisibilityTimeoutExpires() throws Exception {
        Queue shortTimeout = Fencepost.queue(dataSource)
          .visibilityTimeout(Duration.ofSeconds(1))
          .build()
          .forName("timeout-redelivery");

        shortTimeout.enqueue("will-expire");

        Message msg = shortTimeout.tryDequeue().get();
        msg.close();

        assertThat(shortTimeout.tryDequeue()).isEmpty();

        Thread.sleep(1_500);

        Optional<Message> redelivered = shortTimeout.tryDequeue();
        assertThat(redelivered).isPresent();
        assertThat(redelivered.get().payload()).isEqualTo("will-expire");
        assertThat(redelivered.get().attempts()).isEqualTo(2);
        redelivered.get().ack();
    }

    private Queue newQueue() {
        return newQueue("test-queue");
    }

    private Queue newQueue(String name) {
        return Fencepost.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName(name);
    }

}
