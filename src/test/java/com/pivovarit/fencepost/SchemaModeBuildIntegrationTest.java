package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.pivovarit.fencepost.queue.Queue;
import com.pivovarit.fencepost.queue.QueueFactory;
import com.pivovarit.fencepost.queue.QueuePublisher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class SchemaModeBuildIntegrationTest {

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
    void dropAll() throws SQLException {
        TestSchema.dropAll(dataSource);
    }

    @Test
    void shouldCreateSchemaViaSessionBuilder() {
        LockFactory<FencedLock> factory = Fencepost.Locks.session(dataSource)
            .schemaMode(SchemaMode.CREATE)
            .build();

        FencedLock lock = factory.forName("test-lock");
        FencingToken token = lock.lock();
        assertThat(token.value()).isGreaterThan(0);
        lock.unlock();
    }

    @Test
    void shouldCreateSchemaViaLeaseBuilder() {
        LockFactory<RenewableLock> factory = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
            .schemaMode(SchemaMode.CREATE)
            .build();

        RenewableLock lock = factory.forName("test-lock");
        lock.lock();
        lock.unlock();
    }

    @Test
    void shouldCreateSchemaViaQueueBuilder() {
        QueueFactory<Queue> factory = Fencepost.Queues.queue(dataSource)
            .schemaMode(SchemaMode.CREATE)
            .visibilityTimeout(Duration.ofSeconds(30))
            .build();

        Queue queue = factory.forName("test-queue");
        queue.enqueue("hello".getBytes(), "test", Map.of());
        assertThat(queue.tryDequeue()).isPresent();
    }

    @Test
    void shouldCreateSchemaViaPublisherBuilder() {
        QueueFactory<QueuePublisher> factory = Fencepost.Queues.publisher(dataSource)
            .schemaMode(SchemaMode.CREATE)
            .build();

        QueuePublisher pub = factory.forName("test-queue");
        pub.publish("hello".getBytes(), "test");
    }

    @Test
    void shouldValidateSchemaViaSessionBuilder() throws SQLException {
        TestSchema.resetLocks(dataSource);

        LockFactory<FencedLock> factory = Fencepost.Locks.session(dataSource)
            .schemaMode(SchemaMode.VALIDATE)
            .build();

        FencedLock lock = factory.forName("test-lock");
        lock.lock();
        lock.unlock();
    }

    @Test
    void shouldValidateSchemaViaQueueBuilder() throws SQLException {
        TestSchema.resetQueue(dataSource);

        QueueFactory<Queue> factory = Fencepost.Queues.queue(dataSource)
            .schemaMode(SchemaMode.VALIDATE)
            .visibilityTimeout(Duration.ofSeconds(30))
            .build();

        Queue queue = factory.forName("test-queue");
        queue.enqueue("hello".getBytes(), "test", Map.of());
        assertThat(queue.tryDequeue()).isPresent();
    }

    @Test
    void shouldFailValidationOnEmptyDatabase() {
        assertThatThrownBy(() -> Fencepost.Locks.session(dataSource)
            .schemaMode(SchemaMode.VALIDATE)
            .build())
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("does not exist");
    }

    @Test
    void shouldFailQueueValidationOnEmptyDatabase() {
        assertThatThrownBy(() -> Fencepost.Queues.queue(dataSource)
            .schemaMode(SchemaMode.VALIDATE)
            .visibilityTimeout(Duration.ofSeconds(30))
            .build())
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("does not exist");
    }
}
