# fencepost

Distributed concurrency toolkit for Java + PostgreSQL.

Zero dependencies beyond `java.sql`. Requires Java 11+.

**Under construction.**

## Features

Fencepost provides three lock strategies and a message queue, all backed by PostgreSQL.

## Lock Types

| Type | Mechanism | Fencing Token | Auto-Expiry | Holds Connection |
|------|-----------|:---:|:---:|:---:|
| `advisory` | PostgreSQL advisory locks | - | - | + |
| `session` | Table-based, `SELECT ... FOR UPDATE` | + | - | + |
| `lease` | Table-based, timestamp TTL + auto-renew | + | + | - |

- **Advisory** - leverages PostgreSQL's built-in advisory locks. No table or schema setup required. Holds a database connection for the duration of the lock. Released automatically on disconnect. Simple and lightweight, but provides no fencing tokens, so it can't protect against stale holders writing to external systems.

- **Session** - uses a dedicated table with `SELECT ... FOR UPDATE` to hold the lock within an open transaction. Issues monotonically increasing fencing tokens on each acquisition. The token lets downstream systems reject writes from holders that have been superseded. Holds a connection for the duration of the lock - if the process crashes, the connection is closed and the lock is released.

- **Lease** - does not hold a connection or transaction. Acquires the lock by writing a timestamp to a table and releases the connection immediately. The lock is held purely via a TTL (`expires_at`) - if a holder crashes, the lock automatically becomes available after the lease duration. An optional auto-renew thread extends the lease periodically to prevent expiry during long-running work. Supports a quiet period to enforce a minimum gap between consecutive acquisitions. Best suited for long-running tasks where occupying a connection pool slot is not acceptable.

## Table Setup

Session and lease locks require a table. Advisory locks don't need any setup.

```sql
CREATE TABLE fencepost_locks (
    lock_name   TEXT PRIMARY KEY,
    token       BIGINT NOT NULL DEFAULT 0,
    locked_by   TEXT,
    locked_at   TIMESTAMP WITH TIME ZONE,
    expires_at  TIMESTAMP WITH TIME ZONE
);
```

The table name defaults to `fencepost_locks` but can be customized via `.tableName("my_locks")` on the builder.

## Examples

### Advisory Lock

```java
Factory<Lock> fencepost = Fencepost.advisoryLock(dataSource).build();
Lock lock = fencepost.forName("my-resource");

lock.lock();                          // blocking
lock.lock(Duration.ofSeconds(5));     // blocking with timeout
lock.tryLock();                       // non-blocking

// convenience wrapper
lock.runLocked(() -> { /* critical section */ });
```

### Session Lock

```java
Factory<FencedLock> fencepost = Fencepost.sessionLock(dataSource).build();
FencedLock lock = fencepost.forName("my-resource");

// fencing token protects against stale writes
lock.runLocked(token -> {
    externalStore.write(data, token.value());
});
```

### Lease Lock

```java
Factory<RenewableLock> fencepost = Fencepost.leaseLock(dataSource, Duration.ofSeconds(30))
    .withAutoRenew(Duration.ofSeconds(10))
    .withQuietPeriod(Duration.ofSeconds(5))
    .onAutoRenewFailure(e -> log.error("auto-renew failed", e))
    .build();

RenewableLock lock = fencepost.forName("my-resource");

FencingToken token = lock.lock();
try {
    longRunningTask(token);
} finally {
    lock.unlock();
}
```

## Docker Compose Example

The `examples/docker-compose` directory contains a ready-to-run demo where three container instances compete to increment a shared counter in PostgreSQL.

```
cd examples/docker-compose
docker compose up --build
```

The output shows each instance racing to acquire the lock. Winners increment the counter; losers skip. At the end of each phase, the final counter value confirms that no updates were lost.

## Queue

Fencepost includes a PostgreSQL-backed message queue with at-least-once delivery, visibility timeouts, and `LISTEN/NOTIFY`-based blocking dequeue.

### Queue Table Setup

```sql
CREATE TABLE fencepost_queue (
    id            BIGSERIAL PRIMARY KEY,
    queue_name    TEXT NOT NULL,
    payload       TEXT NOT NULL,
    type          TEXT,
    headers       JSONB,
    visible_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    picked_by     TEXT,
    attempts      INT NOT NULL DEFAULT 0
);
```

The table name defaults to `fencepost_queue` but can be customized via `.tableName("my_queue")` on the builder.

### Queue Example

```java
Factory<Queue> fencepost = Fencepost.queue(dataSource)
    .visibilityTimeout(Duration.ofSeconds(30)) // required
    .build();

Queue queue = fencepost.forName("my-queue");

queue.enqueue("hello");
queue.enqueue("delayed hello", Duration.ofSeconds(10));

// enqueue with type and headers
queue.enqueue("{\"to\":\"user@example.com\"}", "send-email.v1", Map.of("priority", "high"));
queue.enqueue("{\"to\":\"user@example.com\"}", "send-email.v1", Map.of("priority", "high"), Duration.ofSeconds(10));

Message msg = queue.dequeue();           // blocking (LISTEN/NOTIFY)
Message msg = queue.dequeue(Duration.ofSeconds(5)); // with timeout
Optional<Message> msg = queue.tryDequeue();         // non-blocking

msg.type();       // "send-email.v1"
msg.headers();    // {"priority": "high"}

// ack() deletes the message, nack() makes it visible again immediately
msg.ack();
msg.nack();
```

Each message can optionally carry a `type` (a plain text label for routing or versioning) and `headers` (a `Map<String, String>` stored as JSONB). Both are nullable - plain `enqueue(payload)` and `enqueue(payload, delay)` still work as before.

If processing fails without calling `ack()` or `nack()`, the message becomes visible again after the visibility timeout expires, with an incremented `attempts` counter.

## Important: PostgreSQL Clock Behavior

PostgreSQL's `clock_timestamp()` / `now()` relies on the system clock, which is **not monotonic** and is subject to clock skew (e.g., NTP adjustments, VM clock drift, leap second handling). This means that timestamp-based lease expiry can, in rare cases, behave unexpectedly - a lease may appear to expire early or late if the database server's clock jumps.

If your use case requires **absolute mutual-exclusion guarantees** (e.g., protecting writes to an external system), you have two options:

1. **Use fencing tokens** - the fencing token is a monotonically increasing value that lets downstream systems reject stale writes, regardless of clock behavior. Pass the token to any external resource and have that resource reject requests with a token lower than the highest it has already seen. This works with both `session` and `lease` locks.

2. **Use a `session` lock instead** - since `session` locks are held via `SELECT ... FOR UPDATE` within an open transaction, they don't depend on timestamps at all and are immune to clock skew. The trade-off is that a session lock holds a database connection for the entire duration of the lock, which may not be acceptable for long-running tasks or applications with limited connection pools.

