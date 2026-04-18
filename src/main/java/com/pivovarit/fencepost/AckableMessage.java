package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.LostOwnershipException;
import com.pivovarit.fencepost.queue.Message;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

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

    private volatile State state = State.ACTIVE;

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
            int updated = Jdbc.update(dataSource, String.format("DELETE FROM %s WHERE id = ? AND picked_by = ?", tableName))
              .bind(id)
              .bind(pickToken)
              .execute();

            if (updated != 1) {
                throw new LostOwnershipException(id);
            }

            state = State.ACKED;
        } catch (SQLException e) {
            throw new FencepostException("Failed to ack message: " + id, e);
        }
    }

    @Override
    public void nack() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Message already " + state.name().toLowerCase());
        }
        try {
            int updated = Jdbc.update(dataSource, String.format("UPDATE %s SET visible_at = now(), picked_by = NULL WHERE id = ? AND picked_by = ?", tableName))
              .bind(id)
              .bind(pickToken)
              .execute();
            if (updated != 1) {
                throw new LostOwnershipException(id);
            }
            state = State.NACKED;
        } catch (SQLException e) {
            throw new FencepostException("Failed to nack message: " + id, e);
        }
    }

    @Override
    public void close() {
        if (state == State.ACTIVE) {
            state = State.CLOSED;
        }
    }
}
