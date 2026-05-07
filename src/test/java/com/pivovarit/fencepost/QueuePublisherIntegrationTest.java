package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Message;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class QueuePublisherIntegrationTest {

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
    void shouldPublishAndDequeueViaQueue() {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        publisher.publish("hello".getBytes(UTF_8), "test.v1");

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("hello".getBytes(UTF_8));
        assertThat(msg.get().type()).hasValue("test.v1");
        msg.get().ack();
    }

    @Test
    void shouldPublishWithDelay() throws Exception {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        publisher.publish("delayed".getBytes(UTF_8), "test.v1", Duration.ofSeconds(2));

        assertThat(queue.tryDequeue()).isEmpty();

        Thread.sleep(2_500);

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("delayed".getBytes(UTF_8));
        msg.get().ack();
    }

    @Test
    void shouldPublishWithTypeAndHeaders() {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        publisher.publish("typed".getBytes(UTF_8), "email.send.v1", Map.of("priority", "high"));

        Message msg = queue.tryDequeue().get();
        assertThat(msg.type()).hasValue("email.send.v1");
        assertThat(msg.headers()).containsEntry("priority", "high");
        msg.ack();
    }

    @Test
    void shouldPublishWithTypeHeadersAndDelay() throws Exception {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        publisher.publish("full".getBytes(UTF_8), "event.v1", Map.of("key", "val"), Duration.ofSeconds(1));

        assertThat(queue.tryDequeue()).isEmpty();

        Thread.sleep(1_500);

        Message msg = queue.tryDequeue().get();
        assertThat(msg.payload()).isEqualTo("full".getBytes(UTF_8));
        assertThat(msg.type()).hasValue("event.v1");
        assertThat(msg.headers()).containsEntry("key", "val");
        msg.ack();
    }

    @Test
    void shouldIsolateNamedQueues() {
        QueueFactory<QueuePublisher> factory = Fencepost.Queues.publisher(dataSource).build();
        QueuePublisher emails = factory.forName("emails");
        QueuePublisher webhooks = factory.forName("webhooks");

        emails.publish("email-1".getBytes(UTF_8), "email.v1");
        webhooks.publish("webhook-1".getBytes(UTF_8), "webhook.v1");

        assertThat(newQueue("emails").tryDequeue().get().payload()).isEqualTo("email-1".getBytes(UTF_8));
        assertThat(newQueue("webhooks").tryDequeue().get().payload()).isEqualTo("webhook-1".getBytes(UTF_8));
    }

    @Test
    void shouldRejectControlCharactersInType() {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");

        assertThatThrownBy(() -> publisher.publish("x".getBytes(UTF_8), "bad\ntype", Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("control character");
    }

    @Test
    void builderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Queues.publisher(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldPublishTransactionallyAndBeVisibleAfterCommit() throws SQLException {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            publisher.transactional(conn).publish("tx-msg".getBytes(UTF_8), "tx.v1");
            conn.commit();
        }

        Optional<Message> msg = queue.tryDequeue();
        assertThat(msg).isPresent();
        assertThat(msg.get().payload()).isEqualTo("tx-msg".getBytes(UTF_8));
        assertThat(msg.get().type()).hasValue("tx.v1");
        msg.get().ack();
    }

    @Test
    void shouldNotBeVisibleAfterRollback() throws SQLException {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            publisher.transactional(conn).publish("rolled-back".getBytes(UTF_8), "tx.v1");
            conn.rollback();
        }

        assertThat(queue.tryDequeue()).isEmpty();
    }

    @Test
    void shouldPublishTransactionallyWithHeadersAndDelay() throws Exception {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            publisher.transactional(conn).publish(
                "full-tx".getBytes(UTF_8), "event.v1",
                Map.of("key", "val"), Duration.ofSeconds(1));
            conn.commit();
        }

        assertThat(queue.tryDequeue()).isEmpty();

        Thread.sleep(1_500);

        Message msg = queue.tryDequeue().get();
        assertThat(msg.payload()).isEqualTo("full-tx".getBytes(UTF_8));
        assertThat(msg.type()).hasValue("event.v1");
        assertThat(msg.headers()).containsEntry("key", "val");
        msg.ack();
    }

    @Test
    void shouldPublishAtomicallyWithBusinessWrite() throws SQLException {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");
        Queue queue = newQueue("test-queue");

        try (Connection setup = dataSource.getConnection()) {
            setup.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test_orders (id BIGSERIAL PRIMARY KEY, data TEXT)");
            setup.createStatement().execute("TRUNCATE test_orders");
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("INSERT INTO test_orders (data) VALUES ('order-1')");
            publisher.transactional(conn).publish("order-created".getBytes(UTF_8), "order.v1");
            conn.commit();
        }

        assertThat(queue.tryDequeue()).isPresent();
        try (Connection check = dataSource.getConnection()) {
            var rs = check.createStatement().executeQuery("SELECT count(*) FROM test_orders");
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void transactionalShouldRejectNullConnection() {
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("test-queue");

        assertThatThrownBy(() -> publisher.transactional(null))
            .isInstanceOf(NullPointerException.class);
    }

    private Queue newQueue(String name) {
        return Fencepost.Queues.queue(dataSource)
            .visibilityTimeout(Duration.ofMinutes(5))
            .build()
            .forName(name);
    }
}
