package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.LostOwnershipException;
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
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
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
    void shouldEnqueueAndTryDequeue() {
        Queue queue = newQueue();
        queue.enqueue("hello".getBytes(UTF_8));

        Optional<Message> msg = queue.tryDequeue();

        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("hello".getBytes(UTF_8));
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
        queue.enqueue("first".getBytes(UTF_8));
        queue.enqueue("second".getBytes(UTF_8));
        queue.enqueue("third".getBytes(UTF_8));

        assertThat(queue.tryDequeue().get().payload()).isEqualTo("first".getBytes(UTF_8));
        assertThat(queue.tryDequeue().get().payload()).isEqualTo("second".getBytes(UTF_8));
        assertThat(queue.tryDequeue().get().payload()).isEqualTo("third".getBytes(UTF_8));
        assertThat(queue.tryDequeue()).isEmpty();
    }

    @Test
    void shouldRespectDelayedEnqueue() throws Exception {
        Queue queue = newQueue();
        queue.enqueue("delayed".getBytes(UTF_8), Duration.ofSeconds(2));

        assertThat(queue.tryDequeue()).isEmpty();

        Thread.sleep(2_500);

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("delayed".getBytes(UTF_8));
        msg.get().ack();
    }

    @Test
    void shouldIsolateNamedQueues() {
        Queue emails = newQueue("emails");
        Queue webhooks = newQueue("webhooks");

        emails.enqueue("email-1".getBytes(UTF_8));
        webhooks.enqueue("webhook-1".getBytes(UTF_8));

        assertThat(emails.tryDequeue().get().payload()).isEqualTo("email-1".getBytes(UTF_8));
        assertThat(webhooks.tryDequeue().get().payload()).isEqualTo("webhook-1".getBytes(UTF_8));
        assertThat(emails.tryDequeue()).isEmpty();
        assertThat(webhooks.tryDequeue()).isEmpty();
    }

    @Test
    void ackShouldDeleteMessage() {
        Queue queue = newQueue();
        queue.enqueue("to-ack".getBytes(UTF_8));

        Message msg = queue.tryDequeue().get();
        msg.ack();

        assertThat(queue.tryDequeue()).isEmpty();
    }

    @Test
    void nackShouldMakeMessageImmediatelyAvailable() {
        Queue queue = newQueue();
        queue.enqueue("to-nack".getBytes(UTF_8));

        Message msg = queue.tryDequeue().get();
        msg.nack();

        Optional<Message> redelivered = queue.tryDequeue();
        assertThat(redelivered).isPresent();
        assertThat(redelivered.get().payload()).isEqualTo("to-nack".getBytes(UTF_8));
        assertThat(redelivered.get().attempts()).isEqualTo(2);
        redelivered.get().ack();
    }

    @Test
    void closeWithoutAckShouldLetVisibilityTimeoutExpire() {
        Queue shortTimeout = Fencepost.queue(dataSource).visibilityTimeout(Duration.ofSeconds(1)).build()
          .forName("test-queue");
        shortTimeout.enqueue("to-expire".getBytes(UTF_8));

        Message msg = shortTimeout.tryDequeue().get();
        msg.close();

        assertThat(shortTimeout.tryDequeue()).isEmpty();
    }

    @Test
    void ackAfterAckShouldThrow() {
        Queue queue = newQueue();
        queue.enqueue("double-ack".getBytes(UTF_8));

        Message msg = queue.tryDequeue().get();
        msg.ack();

        assertThatThrownBy(msg::ack)
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void nackAfterAckShouldThrow() {
        Queue queue = newQueue();
        queue.enqueue("ack-then-nack".getBytes(UTF_8));

        Message msg = queue.tryDequeue().get();
        msg.ack();

        assertThatThrownBy(msg::nack)
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void concurrentAckShouldNotReportLostOwnership() throws Exception {
        Queue queue = newQueue();

        int iterations = 50;
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicInteger successes = new AtomicInteger();

        for (int i = 0; i < iterations; i++) {
            queue.enqueue(("race-" + i).getBytes(UTF_8));
            Message msg = queue.tryDequeue().get();

            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch done = new CountDownLatch(2);

            Runnable acker = () -> {
                try {
                    barrier.await();
                    msg.ack();
                    successes.incrementAndGet();
                } catch (IllegalStateException | LostOwnershipException e) {
                    failures.add(e);
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            };

            new Thread(acker).start();
            new Thread(acker).start();

            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(successes.get())
          .as("each concurrent-ack iteration should have exactly one winner")
          .isEqualTo(iterations);

        assertThat(failures)
          .as("a thread that loses a purely local ack() race must be told it was late locally (IllegalStateException) - not that the DB row was stolen (LostOwnershipException), because no other consumer was involved")
          .hasSize(iterations)
          .allMatch(t -> t instanceof IllegalStateException);
    }

    @Test
    void ackAfterNackShouldThrow() {
        Queue queue = newQueue();
        queue.enqueue("nack-then-ack".getBytes(UTF_8));

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

        queue.enqueue("wake-up".getBytes(UTF_8));

        assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ref.get().payload()).isEqualTo("wake-up".getBytes(UTF_8));
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
        queue.enqueue("already-here".getBytes(UTF_8));

        Message msg = queue.dequeue(Duration.ofSeconds(5));
        assertThat(msg.payload()).isEqualTo("already-here".getBytes(UTF_8));
        msg.ack();
        queue.close();
    }

    @Test
    void concurrentConsumersShouldNotReceiveSameMessage() throws Exception {
        Queue queue = newQueue();
        int messageCount = 50;
        for (int i = 0; i < messageCount; i++) {
            queue.enqueue(("msg-" + i).getBytes(UTF_8));
        }

        List<String> consumed = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(messageCount);

        Runnable consumer = () -> {
            while (true) {
                Optional<Message> msg = queue.tryDequeue();
                if (msg.isEmpty()) {
                    break;
                }
                consumed.add(new String(msg.get().payload(), UTF_8));
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
        queue.enqueue("via-builder".getBytes(UTF_8));

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("via-builder".getBytes(UTF_8));
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
    void ackFromStaleConsumerShouldNotDeleteMessageOwnedByAnother() throws Exception {
        Queue queue = newQueue();
        queue.enqueue("contested".getBytes(UTF_8));

        Message msgA = queue.tryDequeue().get();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_queue SET visible_at = now() - interval '1 second' WHERE id = " + msgA.id());
        }

        Message msgB = queue.tryDequeue().get();
        assertThat(msgB.id()).isEqualTo(msgA.id());

        assertThatThrownBy(msgA::ack)
          .as("stale consumer A must be told its ack lost ownership")
          .isInstanceOf(LostOwnershipException.class);

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT COUNT(*) FROM fencepost_queue WHERE id = " + msgB.id())) {
            rs.next();
            assertThat(rs.getInt(1))
              .as("stale consumer A must not delete a message now owned by consumer B")
              .isEqualTo(1);
        }

        msgB.ack();
    }

    @Test
    void nackShouldStillBeCallableAfterAckLosesOwnership() throws Exception {
        Queue queue = newQueue();
        queue.enqueue("contested".getBytes(UTF_8));

        Message msgA = queue.tryDequeue().get();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_queue SET visible_at = now() - interval '1 second' WHERE id = " + msgA.id());
        }

        Message msgB = queue.tryDequeue().get();

        assertThatThrownBy(msgA::ack).isInstanceOf(LostOwnershipException.class);

        assertThatThrownBy(msgA::nack)
          .as("after failed ack, nack must not throw IllegalStateException")
          .isInstanceOf(LostOwnershipException.class);

        msgB.ack();
    }

    @Test
    void nackFromStaleConsumerShouldNotAffectMessageOwnedByAnother() throws Exception {
        Queue queue = newQueue();
        queue.enqueue("contested-nack".getBytes(UTF_8));

        Message msgA = queue.tryDequeue().get();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_queue SET visible_at = now() - interval '1 second' WHERE id = " + msgA.id());
        }

        Message msgB = queue.tryDequeue().get();
        assertThat(msgB.id()).isEqualTo(msgA.id());

        assertThatThrownBy(msgA::nack)
          .as("stale consumer A must be told its nack lost ownership")
          .isInstanceOf(LostOwnershipException.class);

        assertThat(queue.tryDequeue())
          .as("stale consumer A's nack must not steal the message back from B")
          .isEmpty();

        msgB.ack();
    }

    @Test
    void messageShouldBecomeVisibleAfterVisibilityTimeoutExpires() throws Exception {
        Queue shortTimeout = Fencepost.queue(dataSource)
          .visibilityTimeout(Duration.ofSeconds(1))
          .build()
          .forName("timeout-redelivery");

        shortTimeout.enqueue("will-expire".getBytes(UTF_8));

        Message msg = shortTimeout.tryDequeue().get();
        msg.close();

        assertThat(shortTimeout.tryDequeue()).isEmpty();

        Thread.sleep(1_500);

        Optional<Message> redelivered = shortTimeout.tryDequeue();
        assertThat(redelivered).isPresent();
        assertThat(redelivered.get().payload()).isEqualTo("will-expire".getBytes(UTF_8));
        assertThat(redelivered.get().attempts()).isEqualTo(2);
        redelivered.get().ack();
    }

    @Test
    void closeShouldNotHangWhenListenerAcquireIsBlocked() throws Exception {
        GatingDataSource gated = new GatingDataSource(dataSource);
        Queue queue = Fencepost.queue(gated)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("close-while-blocked");

        // Allow tryDequeue's getConnection to succeed, then block subsequent acquisitions
        // so the listener connection acquisition inside ensureListening() hangs.
        gated.allowThenBlock(1);

        AtomicReference<Throwable> consumerError = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                queue.dequeue();
            } catch (Throwable t) {
                consumerError.set(t);
            }
        }, "consumer");
        consumer.start();

        assertThat(gated.awaitBlocked(2, TimeUnit.SECONDS))
          .as("consumer should be parked inside dataSource.getConnection()")
          .isTrue();

        long start = System.nanoTime();
        queue.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs)
          .as("close() must not wait for the stuck listener acquisition")
          .isLessThan(1000);

        gated.release();
        consumer.join(5000);
        assertThat(consumer.isAlive()).isFalse();
        assertThat(consumerError.get()).isInstanceOf(FencepostException.class);
        assertThat(gated.outstanding())
          .as("a connection acquired after close() must not leak")
          .isZero();
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

    static final class GatingDataSource implements DataSource {
        private final DataSource delegate;
        private final AtomicInteger outstanding = new AtomicInteger(0);
        private final AtomicInteger remainingAllowed = new AtomicInteger(Integer.MAX_VALUE);
        private final CountDownLatch blocked = new CountDownLatch(1);
        private volatile CountDownLatch gate;

        GatingDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        void allowThenBlock(int allowed) {
            remainingAllowed.set(allowed);
            gate = new CountDownLatch(1);
        }

        boolean awaitBlocked(long timeout, TimeUnit unit) throws InterruptedException {
            return blocked.await(timeout, unit);
        }

        void release() {
            CountDownLatch g = this.gate;
            if (g != null) g.countDown();
        }

        int outstanding() {
            return outstanding.get();
        }

        @Override
        public Connection getConnection() throws SQLException {
            CountDownLatch g = this.gate;
            if (g != null && remainingAllowed.getAndDecrement() <= 0) {
                blocked.countDown();
                try {
                    g.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("getConnection interrupted", e);
                }
            }
            Connection raw = delegate.getConnection();
            outstanding.incrementAndGet();
            return (Connection) Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[]{Connection.class},
              tracking(raw));
        }

        private InvocationHandler tracking(Connection raw) {
            return (proxy, method, args) -> {
                if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                    try {
                        return method.invoke(raw, args);
                    } finally {
                        outstanding.decrementAndGet();
                    }
                }
                try {
                    return method.invoke(raw, args);
                } catch (java.lang.reflect.InvocationTargetException ite) {
                    throw ite.getCause();
                }
            };
        }

        @Override
        public Connection getConnection(String u, String p) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }
}
