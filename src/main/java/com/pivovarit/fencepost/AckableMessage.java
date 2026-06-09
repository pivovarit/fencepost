package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.AckUnknownException;
import com.pivovarit.fencepost.queue.LostOwnershipException;
import com.pivovarit.fencepost.queue.Message;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

final class AckableMessage implements Message {

    private enum State {ACTIVE, ACKED, NACKED, DEAD_LETTERED, CLOSED, LOST}

    private final long id;
    private final byte[] payload;
    private final String type;
    private final Map<String, String> headers;
    private final int attempts;
    private final String pickToken;
    private final DataSource dataSource;
    private final Sql sql;

    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

    AckableMessage(long id, byte[] payload, String type, Map<String, String> headers, int attempts, String pickToken, DataSource dataSource, Sql sql) {
        this.id = id;
        this.payload = payload;
        this.type = type;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Map.of();
        this.attempts = attempts;
        this.pickToken = pickToken;
        this.dataSource = dataSource;
        this.sql = sql;
    }

    static final class Sql {
        final String ack;
        final String nack;
        final String deadLetter;

        Sql(String tableName) {
            this.ack = String.format("DELETE FROM %s WHERE id = ? AND picked_by = ?", tableName);
            this.nack = String.format("UPDATE %s SET visible_at = now() + %s, picked_by = NULL WHERE id = ? AND picked_by = ?", tableName, Jdbc.intervalMillis());
            this.deadLetter = String.format("UPDATE %s SET dead_at = now(), picked_by = NULL, last_error = ? WHERE id = ? AND picked_by = ?", tableName);
        }
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public Optional<String> type() {
        return Optional.ofNullable(type);
    }

    @Override
    public Map<String, String> headers() {
        return headers;
    }

    @Override
    public int attempts() {
        return attempts;
    }

    @Override
    public void ack() {
        if (!state.compareAndSet(State.ACTIVE, State.ACKED)) {
            throw new IllegalStateException("Message already " + state.get().name().toLowerCase());
        }
        try {
            int updated = Jdbc.update(dataSource, sql.ack)
              .bind(id)
              .bind(pickToken)
              .execute();

            if (updated != 1) {
                state.set(State.LOST);
                throw new LostOwnershipException(id);
            }
        } catch (SQLException e) {
            state.set(State.ACTIVE);
            throw new AckUnknownException(id, "ack", e);
        } catch (LostOwnershipException e) {
            throw e;
        } catch (Exception e) {
            state.set(State.ACTIVE);
            throw e;
        }
    }

    @Override
    public void nack() {
        nack(Duration.ZERO);
    }

    void nack(Duration delay) {
        if (!state.compareAndSet(State.ACTIVE, State.NACKED)) {
            throw new IllegalStateException("Message already " + state.get().name().toLowerCase());
        }
        long delayMillis = Durations.toNonNegativeMillis(delay, "delay");
        try {
            int updated = Jdbc.update(dataSource, sql.nack)
              .bind(delayMillis)
              .bind(id)
              .bind(pickToken)
              .execute();
            if (updated != 1) {
                state.set(State.LOST);
                throw new LostOwnershipException(id);
            }
        } catch (SQLException e) {
            state.set(State.ACTIVE);
            throw new AckUnknownException(id, "nack", e);
        } catch (LostOwnershipException e) {
            throw e;
        } catch (Exception e) {
            state.set(State.ACTIVE);
            throw e;
        }
    }

    void deadLetter(String reason) {
        if (!state.compareAndSet(State.ACTIVE, State.DEAD_LETTERED)) {
            throw new IllegalStateException("Message already " + state.get().name().toLowerCase());
        }
        try {
            int updated = Jdbc.update(dataSource, sql.deadLetter)
              .bind(reason)
              .bind(id)
              .bind(pickToken)
              .execute();
            if (updated != 1) {
                state.set(State.LOST);
                throw new LostOwnershipException(id);
            }
        } catch (SQLException e) {
            state.set(State.ACTIVE);
            throw new AckUnknownException(id, "deadLetter", e);
        } catch (LostOwnershipException e) {
            throw e;
        } catch (Exception e) {
            state.set(State.ACTIVE);
            throw e;
        }
    }

    @Override
    public void close() {
        state.compareAndSet(State.ACTIVE, State.CLOSED);
    }
}
