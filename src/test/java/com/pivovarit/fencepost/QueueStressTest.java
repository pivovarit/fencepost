package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class QueueStressTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static HikariDataSource dataSource;

    @BeforeAll
    static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(PG.getJdbcUrl());
        config.setUsername(PG.getUsername());
        config.setPassword(PG.getPassword());
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(5000);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void createTable() throws SQLException {
        TestSchema.resetQueue(dataSource);
    }

    @Test
    void noDoubleDeliveryUnderConcurrentConsumers() throws Exception {
        Queue queue = newQueue();
        for (int i = 0; i < 500; i++) {
            queue.enqueue(("msg-" + i).getBytes(UTF_8), "test", Map.of());
        }

        List<Long> consumedIds = new CopyOnWriteArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch done = new CountDownLatch(10);

        for (int t = 0; t < 10; t++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        Optional<Message> msg = queue.tryDequeue();
                        if (msg.isEmpty()) {
                            break;
                        }
                        consumedIds.add(msg.get().id());
                        msg.get().ack();
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(consumedIds).hasSize(500);
        assertThat(consumedIds).doesNotHaveDuplicates();
    }

    @Test
    void noMessageLossWithNackRedelivery() throws Exception {
        Queue queue = newQueue();
        for (int i = 0; i < 100; i++) {
            queue.enqueue(("msg-" + i).getBytes(UTF_8), "test", Map.of());
        }

        AtomicInteger nackCount = new AtomicInteger();
        AtomicInteger ackCount = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch done = new CountDownLatch(5);

        for (int t = 0; t < 5; t++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        Optional<Message> msg = queue.tryDequeue();
                        if (msg.isEmpty()) {
                            break;
                        }
                        if (msg.get().attempts() == 1) {
                            msg.get().nack();
                            nackCount.incrementAndGet();
                        } else {
                            msg.get().ack();
                            ackCount.incrementAndGet();
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        while (true) {
            Optional<Message> msg = queue.tryDequeue();
            if (msg.isEmpty()) {
                break;
            }
            if (msg.get().attempts() == 1) {
                msg.get().nack();
                nackCount.incrementAndGet();
            } else {
                msg.get().ack();
                ackCount.incrementAndGet();
            }
        }

        assertThat(nackCount.get()).isEqualTo(100);
        assertThat(ackCount.get()).isEqualTo(100);
    }

    @Test
    void concurrentProducersAndConsumers() throws Exception {
        List<Long> consumedIds = new CopyOnWriteArrayList<>();
        AtomicBoolean producersFinished = new AtomicBoolean(false);

        ExecutorService producers = Executors.newFixedThreadPool(5);
        ExecutorService consumers = Executors.newFixedThreadPool(5);
        CountDownLatch producersDone = new CountDownLatch(5);
        CountDownLatch consumersDone = new CountDownLatch(5);

        for (int p = 0; p < 5; p++) {
            int producerId = p;
            producers.submit(() -> {
                try {
                    Queue queue = newQueue();
                    for (int i = 0; i < 100; i++) {
                        queue.enqueue(("p" + producerId + "-msg-" + i).getBytes(UTF_8), "test", Map.of());
                    }
                } finally {
                    producersDone.countDown();
                }
            });
        }

        for (int c = 0; c < 5; c++) {
            consumers.submit(() -> {
                try {
                    Queue queue = newQueue();
                    while (true) {
                        Optional<Message> msg = queue.tryDequeue();
                        if (msg.isPresent()) {
                            consumedIds.add(msg.get().id());
                            msg.get().ack();
                        } else {
                            if (producersFinished.get()) {
                                break;
                            }
                            Thread.sleep(10);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    consumersDone.countDown();
                }
            });
        }

        assertThat(producersDone.await(60, TimeUnit.SECONDS)).isTrue();
        producers.shutdown();
        producersFinished.set(true);

        Thread.sleep(2000);

        Queue drainQueue = newQueue();
        while (true) {
            Optional<Message> msg = drainQueue.tryDequeue();
            if (msg.isEmpty()) {
                break;
            }
            consumedIds.add(msg.get().id());
            msg.get().ack();
        }

        assertThat(consumersDone.await(60, TimeUnit.SECONDS)).isTrue();
        consumers.shutdown();

        assertThat(consumedIds).hasSize(500);
        assertThat(consumedIds).doesNotHaveDuplicates();
    }

    @Test
    void connectionPoolStabilityUnderBurst() throws Exception {
        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch done = new CountDownLatch(10);

        for (int t = 0; t < 10; t++) {
            executor.submit(() -> {
                try {
                    Queue queue = newQueue();
                    for (int i = 0; i < 50; i++) {
                        queue.enqueue(("burst-" + i).getBytes(UTF_8), "test", Map.of());
                        queue.tryDequeue().ifPresent(Message::ack);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    done.countDown();
                }
            });
        }

        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(exceptions).isEmpty();
        assertThat(dataSource.getHikariPoolMXBean().getActiveConnections()).isEqualTo(0);
    }

    private Queue newQueue() {
        return Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("stress-queue");
    }
}
