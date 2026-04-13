package com.pivovarit.fencepost.dashboard;

import com.pivovarit.fencepost.FencepostDashboard;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.pivovarit.fencepost.Fencepost;
import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class FencepostDashboardTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource dataSource;

    FencepostDashboard dashboard;

    @BeforeAll
    static void setupDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;
    }

    @BeforeEach
    void createTables() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "DROP TABLE IF EXISTS fencepost_queue; DROP TABLE IF EXISTS fencepost_locks;" +
              "CREATE TABLE fencepost_locks (" +
              "  lock_name TEXT PRIMARY KEY," +
              "  token BIGINT NOT NULL DEFAULT 0," +
              "  locked_by TEXT," +
              "  locked_at TIMESTAMPTZ," +
              "  expires_at TIMESTAMPTZ" +
              ");" +
              "CREATE TABLE fencepost_queue (" +
              "  id BIGSERIAL PRIMARY KEY," +
              "  queue_name TEXT NOT NULL," +
              "  payload BYTEA NOT NULL," +
              "  type TEXT," +
              "  headers JSONB," +
              "  created_at TIMESTAMPTZ NOT NULL DEFAULT now()," +
              "  visible_at TIMESTAMPTZ NOT NULL DEFAULT now()," +
              "  attempts INT NOT NULL DEFAULT 0," +
              "  picked_by TEXT" +
              ")"
            );
        }
    }

    @AfterEach
    void stopDashboard() {
        if (dashboard != null) {
            dashboard.stop();
        }
    }

    @Test
    void shouldStartAndServeStatusEndpoint() throws Exception {
        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        String response = httpGet("http://localhost:" + dashboard.getPort() + "/api/status");

        assertThat(response).contains("\"locks_enabled\":true");
        assertThat(response).contains("\"queues_enabled\":true");
    }

    @Test
    void shouldServeLocksEndpoint() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_locks (lock_name, token, locked_by, locked_at, expires_at) " +
              "VALUES ('test-lock', 5, 'worker-1', now(), now() + interval '1 hour')"
            );
        }

        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        String response = httpGet("http://localhost:" + dashboard.getPort() + "/api/locks");

        assertThat(response).contains("\"name\":\"test-lock\"");
        assertThat(response).contains("\"token\":5");
        assertThat(response).contains("\"locked_by\":\"worker-1\"");
    }

    @Test
    void shouldServeSingleLockEndpoint() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_locks (lock_name, token) VALUES ('a', 1), ('b', 2)"
            );
        }

        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        String response = httpGet("http://localhost:" + dashboard.getPort() + "/api/locks/b");

        assertThat(response).contains("\"name\":\"b\"");
        assertThat(response).doesNotContain("\"name\":\"a\"");
    }

    @Test
    void shouldServeQueuesEndpoint() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_queue (queue_name, payload, visible_at) " +
              "VALUES ('my-queue', 'hello'::bytea, now() - interval '1 second')"
            );
        }

        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        String response = httpGet("http://localhost:" + dashboard.getPort() + "/api/queues");

        assertThat(response).contains("\"name\":\"my-queue\"");
        assertThat(response).contains("\"total\":1");
    }

    @Test
    void shouldServeQueueDetailEndpoint() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_queue (queue_name, payload, picked_by, attempts) " +
              "VALUES ('tasks', 'do something'::bytea, 'worker-42', 3)"
            );
        }

        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        String response = httpGet("http://localhost:" + dashboard.getPort() + "/api/queues/tasks");

        assertThat(response).contains("\"name\":\"tasks\"");
        assertThat(response).contains("\"in_flight\":1");
        assertThat(response).contains("\"picked_by\":\"worker-42\"");
        assertThat(response).contains("\"attempts\":3");
    }

    @Test
    void shouldServeMessageDetailEndpoint() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_queue (id, queue_name, payload, picked_by, attempts) " +
              "VALUES (1, 'tasks', 'full payload content here'::bytea, 'worker-42', 3)"
            );
        }

        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        String response = httpGet("http://localhost:" + dashboard.getPort() + "/api/queues/tasks/messages/1");

        assertThat(response).contains("\"id\":1");
        assertThat(response).contains("\"payload\":\"ZnVsbCBwYXlsb2FkIGNvbnRlbnQgaGVyZQ==\"");
        assertThat(response).contains("\"picked_by\":\"worker-42\"");
        assertThat(response).contains("\"attempts\":3");
        assertThat(response).contains("\"status\":\"in_flight\"");
    }

    @Test
    void shouldReturn404ForNonexistentMessage() throws Exception {
        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + dashboard.getPort() + "/api/queues/tasks/messages/999").openConnection();
        assertThat(conn.getResponseCode()).isEqualTo(404);
    }

    @Test
    void shouldServeDashboardHtml() throws Exception {
        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + dashboard.getPort() + "/").openConnection();
        conn.setRequestMethod("GET");
        int status = conn.getResponseCode();
        String contentType = conn.getContentType();

        assertThat(status).isEqualTo(200);
        assertThat(contentType).containsIgnoringCase("text/html");
    }

    @Test
    void shouldReturn404ForUnknownPath() throws Exception {
        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + dashboard.getPort() + "/api/unknown").openConnection();
        conn.setRequestMethod("GET");
        int status = conn.getResponseCode();

        assertThat(status).isEqualTo(404);
    }

    @Test
    void shouldReturn404ForNonexistentLock() throws Exception {
        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + dashboard.getPort() + "/api/locks/nonexistent").openConnection();
        assertThat(conn.getResponseCode()).isEqualTo(404);
    }

    @Test
    void shouldWorkWithCustomTableNames() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS fencepost_locks");
            conn.createStatement().execute("DROP TABLE IF EXISTS fencepost_queue");
            conn.createStatement().execute("DROP TABLE IF EXISTS custom_locks");
            conn.createStatement().execute(
              "CREATE TABLE custom_locks ("
                + "lock_name TEXT PRIMARY KEY,"
                + "token BIGINT NOT NULL DEFAULT 0,"
                + "locked_by TEXT,"
                + "locked_at TIMESTAMPTZ,"
                + "expires_at TIMESTAMPTZ)"
            );
            conn.createStatement().execute(
              "INSERT INTO custom_locks (lock_name, token) VALUES ('x', 42)"
            );
        }

        dashboard = new FencepostDashboard(dataSource, "custom_locks", "fencepost_queue");
        dashboard.start(0);

        String statusResponse = httpGet("http://localhost:" + dashboard.getPort() + "/api/status");
        assertThat(statusResponse).contains("\"locks_enabled\":true");
        assertThat(statusResponse).contains("\"queues_enabled\":false");

        String locksResponse = httpGet("http://localhost:" + dashboard.getPort() + "/api/locks");
        assertThat(locksResponse).contains("\"name\":\"x\"");
        assertThat(locksResponse).contains("\"token\":42");
    }

    @Test
    void shouldStreamSseEventsOnNotify() throws Exception {
        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(0);

        HttpURLConnection conn = (HttpURLConnection) new URL(String.format("http://localhost:%d/api/events", dashboard.getPort())).openConnection();
        conn.setRequestMethod("GET");
        conn.setReadTimeout(5000);

        assertThat(conn.getResponseCode()).isEqualTo(200);
        assertThat(conn.getContentType()).containsIgnoringCase("text/event-stream");

        try (var reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String firstLine = reader.readLine();
            assertThat(firstLine).isEqualTo("data: connected");

            try (Connection pgConn = dataSource.getConnection()) {
                pgConn.createStatement().execute("NOTIFY fencepost_dashboard");
            }

            Thread.sleep(200);
            String refreshLine = reader.readLine();
            while (refreshLine != null && refreshLine.isEmpty()) {
                refreshLine = reader.readLine();
            }
            assertThat(refreshLine).isEqualTo("data: refresh");
        }
    }

    @Test
    @Disabled("manual test - run from IDE to experiment with the dashboard in a browser")
    void sandbox() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_locks (lock_name, token, locked_by, locked_at, expires_at) VALUES " +
              "('order-processing', 7, 'worker-1/main', now(), now() + interval '5 minutes')," +
              "('inventory-sync', 3, NULL, NULL, NULL)," +
              "('report-generation', 12, 'worker-2/pool-1', now() - interval '10 minutes', now() - interval '5 minutes')"
            );
        }

        var factory = Fencepost.queue(dataSource)
          .visibilityTimeout(Duration.ofSeconds(30))
          .build();

        Thread producer = new Thread(() -> {
            Queue q = factory.forName("emails");
            int i = 0;
            while (!Thread.currentThread().isInterrupted()) {
                q.enqueue(("email-task-" + (++i)).getBytes(StandardCharsets.UTF_8), "send-email-command.v1", Map.of("priority", "high"));
                sleep(500);
            }
            q.close();
        }, "producer");

        Thread[] consumers = new Thread[3];
        for (int c = 0; c < consumers.length; c++) {
            int id = c + 1;
            consumers[c] = new Thread(() -> {
                Queue q = factory.forName("emails");
                while (!Thread.currentThread().isInterrupted()) {
                    try (Message msg = q.tryDequeue().orElse(null)) {
                        if (msg != null) {
                            sleep(3000);
                            msg.ack();
                        } else {
                            sleep(500);
                        }
                    }
                }
                q.close();
            }, "consumer-" + id);
        }

        producer.start();
        for (Thread consumer : consumers) {
            consumer.start();
        }

        dashboard = new FencepostDashboard(dataSource);
        dashboard.start(3388);
        System.out.println("Dashboard running at http://localhost:3388");
        Thread.currentThread().join();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String httpGet(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
