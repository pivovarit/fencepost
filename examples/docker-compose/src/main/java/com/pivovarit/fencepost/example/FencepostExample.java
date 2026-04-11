package com.pivovarit.fencepost.example;

import com.pivovarit.fencepost.AdvisoryLock;
import com.pivovarit.fencepost.Factory;
import com.pivovarit.fencepost.FencedLock;
import com.pivovarit.fencepost.Fencepost;
import com.pivovarit.fencepost.FencingToken;
import com.pivovarit.fencepost.Message;
import com.pivovarit.fencepost.Queue;
import com.pivovarit.fencepost.RenewableLock;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

public class FencepostExample {

    private static final String NODE = System.getenv().getOrDefault("HOSTNAME", "unknown");
    private static final int ROUNDS = 5;

    public static void main(String[] args) throws Exception {
        DataSource dataSource = dataSource();
        String lockType = System.getenv().getOrDefault("LOCK_TYPE", "advisory");

        initCounter(dataSource, lockType);

        switch (lockType) {
            case "advisory":
                advisoryLockExample(dataSource);
                break;
            case "session":
                createLockTable(dataSource);
                sessionLockExample(dataSource);
                break;
            case "lease":
                createLockTable(dataSource);
                leaseLockExample(dataSource);
                break;
            case "queue":
                createQueueTable(dataSource);
                queueExample(dataSource);
                break;
            default:
                throw new IllegalArgumentException("Unknown LOCK_TYPE: " + lockType);
        }

        if (!"queue".equals(lockType)) {
            int finalValue = readCounter(dataSource, lockType);
            log("[" + lockType + "] final counter value: " + finalValue);
        }
    }

    private static void advisoryLockExample(DataSource dataSource) {
        Factory<AdvisoryLock> factory = Fencepost.advisoryLock(dataSource).build();

        for (int round = 1; round <= ROUNDS; round++) {
            AdvisoryLock lock = factory.forName("counter-lock");

            if (lock.tryLock()) {
                try {
                    int value = incrementCounter(dataSource, "advisory");
                    log("[advisory] round " + round + " - incremented counter to " + value);
                    sleep(2000);
                } finally {
                    lock.unlock();
                }
            } else {
                log("[advisory] round " + round + " - lock held by another instance, skipping");
            }
            sleep(1000);
        }
    }

    private static void sessionLockExample(DataSource dataSource) {
        Factory<FencedLock> factory = Fencepost.sessionLock(dataSource).build();

        for (int round = 1; round <= ROUNDS; round++) {
            FencedLock lock = factory.forName("counter-lock");

            Optional<FencingToken> maybe = lock.tryLock();
            if (maybe.isPresent()) {
                try {
                    int value = incrementCounter(dataSource, "session");
                    log("[session]  round " + round + " - token=" + maybe.get().value() + ", incremented counter to " + value);
                    sleep(2000);
                } finally {
                    lock.unlock();
                }
            } else {
                log("[session]  round " + round + " - lock held by another instance, skipping");
            }
            sleep(1000);
        }
    }

    private static void leaseLockExample(DataSource dataSource) {
        Factory<RenewableLock> factory = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofSeconds(3))
          .onAutoRenewFailure(e -> log("[lease]    auto-renew failed: " + e.getMessage()))
          .build();

        for (int round = 1; round <= ROUNDS; round++) {
            RenewableLock lock = factory.forName("counter-lock");

            Optional<FencingToken> maybe = lock.tryLock();
            if (maybe.isPresent()) {
                try {
                    int value = incrementCounter(dataSource, "lease");
                    log("[lease]    round " + round + " - token=" + maybe.get().value() + ", incremented counter to " + value);
                    sleep(3000);
                } finally {
                    lock.unlock();
                }
            } else {
                log("[lease]    round " + round + " - lock held by another instance, skipping");
            }
            sleep(1000);
        }
    }

    private static void queueExample(DataSource dataSource) {
        Factory<Queue> factory = Fencepost.queue(dataSource)
          .visibilityTimeout(Duration.ofSeconds(30))
          .build();

        Queue queue = factory.forName("tasks");

        for (int i = 1; i <= ROUNDS; i++) {
            String payload = "task-" + i + "-from-" + NODE;
            queue.enqueue(payload);
            log("[queue]    enqueued: " + payload);
        }

        sleep(1000);

        for (int i = 0; i < ROUNDS * 3; i++) {
            try (Message msg = queue.tryDequeue().orElse(null)) {
                if (msg == null) {
                    log("[queue]    no more messages");
                    break;
                }
                log("[queue]    dequeued: " + msg.payload() + " (attempt #" + msg.attempts() + ")");
                sleep(500);
                msg.ack();
                log("[queue]    acked: " + msg.payload());
            }
        }

        queue.close();
    }

    private static void initCounter(DataSource dataSource, String name) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INT NOT NULL DEFAULT 0)");
            try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO counters (name, value) VALUES (?, 0) ON CONFLICT (name) DO NOTHING")) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
        }
    }

    private static int incrementCounter(DataSource dataSource, String name) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
               "UPDATE counters SET value = value + 1 WHERE name = ? RETURNING value")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static int readCounter(DataSource dataSource, String name) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
               "SELECT value FROM counters WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private static void log(String message) {
        System.out.println("[" + NODE + "] " + message);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(System.getenv("DB_URL"));
        ds.setUser(System.getenv("DB_USER"));
        ds.setPassword(System.getenv("DB_PASSWORD"));
        return ds;
    }

    private static void createQueueTable(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS fencepost_queue ("
                + "id          BIGSERIAL PRIMARY KEY,"
                + "queue_name  TEXT NOT NULL,"
                + "payload     TEXT NOT NULL,"
                + "created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "visible_at  TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "attempts    INT NOT NULL DEFAULT 0,"
                + "picked_by   TEXT"
                + ");"
                + "CREATE INDEX IF NOT EXISTS idx_fencepost_queue_dequeue "
                + "ON fencepost_queue (queue_name, visible_at)");
        }
    }

    private static void createLockTable(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS fencepost_locks ("
                + "lock_name   TEXT PRIMARY KEY,"
                + "token       BIGINT NOT NULL DEFAULT 0,"
                + "locked_by   TEXT,"
                + "locked_at   TIMESTAMP WITH TIME ZONE,"
                + "expires_at  TIMESTAMP WITH TIME ZONE"
                + ")");
        }
    }
}
