package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.AckUnknownException;
import com.pivovarit.fencepost.queue.LostOwnershipException;
import com.pivovarit.fencepost.queue.Message;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

final class AckableMessage implements Message {

    private enum State {ACTIVE, ACKED, NACKED, CLOSED}

    private final long id;
    private final byte[] payload;
    private final String type;
    private final Map<String, String> headers;
    private final int attempts;
    private final String pickToken;
    private final DataSource dataSource;
    private final String tableName;

    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

    AckableMessage(long id, byte[] payload, String type, Map<String, String> headers, int attempts, String pickToken, DataSource dataSource, String tableName) {
        this.id = id;
        this.payload = payload;
        this.type = type;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Map.of();
        this.attempts = attempts;
        this.pickToken = pickToken;
        this.dataSource = dataSource;
        this.tableName = tableName;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public byte[] payload() {
        return payload;
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
            int updated = Jdbc.update(dataSource, String.format("DELETE FROM %s WHERE id = ? AND picked_by = ?", tableName))
              .bind(id)
              .bind(pickToken)
              .execute();

            if (updated != 1) {
                state.set(State.ACTIVE);
                throw new LostOwnershipException(id);
            }
        } catch (SQLException e) {
            state.set(State.ACTIVE);
            throw new AckUnknownException(id, "ack", e);
        }
    }

    @Override
    public void nack() {
        if (!state.compareAndSet(State.ACTIVE, State.NACKED)) {
            throw new IllegalStateException("Message already " + state.get().name().toLowerCase());
        }
        try {
            int updated = Jdbc.update(dataSource, String.format("UPDATE %s SET visible_at = now(), picked_by = NULL WHERE id = ? AND picked_by = ?", tableName))
              .bind(id)
              .bind(pickToken)
              .execute();
            if (updated != 1) {
                state.set(State.ACTIVE);
                throw new LostOwnershipException(id);
            }
        } catch (SQLException e) {
            state.set(State.ACTIVE);
            throw new AckUnknownException(id, "nack", e);
        }
    }

    @Override
    public void close() {
        state.compareAndSet(State.ACTIVE, State.CLOSED);
    }
}
