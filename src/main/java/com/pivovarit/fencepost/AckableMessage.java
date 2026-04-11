package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Message;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

final class AckableMessage implements Message {

    private enum State {ACTIVE, ACKED, NACKED, CLOSED}

    private final long id;
    private final String payload;
    private final String type;
    private final Map<String, String> headers;
    private final int attempts;
    private final DataSource dataSource;
    private final String tableName;

    private State state = State.ACTIVE;

    AckableMessage(long id, String payload, String type, Map<String, String> headers, int attempts, DataSource dataSource, String tableName) {
        this.id = id;
        this.payload = payload;
        this.type = type;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Map.of();
        this.attempts = attempts;
        this.dataSource = dataSource;
        this.tableName = tableName;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public String payload() {
        return payload;
    }

    @Override
    public String type() {
        return type;
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
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Message already " + state.name().toLowerCase());
        }
        try {
            Jdbc.update(dataSource, String.format("DELETE FROM %s WHERE id = ? AND picked_by IS NOT NULL", tableName))
              .bind(id)
              .execute();
        } catch (SQLException e) {
            throw new FencepostException("Failed to ack message: " + id, e);
        }
        state = State.ACKED;
    }

    @Override
    public void nack() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Message already " + state.name().toLowerCase());
        }
        try {
            Jdbc.update(dataSource, String.format("UPDATE %s SET visible_at = now(), picked_by = NULL WHERE id = ?", tableName))
              .bind(id)
              .execute();
        } catch (SQLException e) {
            throw new FencepostException("Failed to nack message: " + id, e);
        }
        state = State.NACKED;
    }

    @Override
    public void close() {
        if (state == State.ACTIVE) {
            state = State.CLOSED;
        }
    }
}
