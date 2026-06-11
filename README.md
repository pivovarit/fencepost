# fencepost

Distributed concurrency toolkit for Java + PostgreSQL.

Zero dependencies beyond `org.postgresql:postgresql`. Requires Java 17+.

## Features

Fencepost provides three lock strategies, leader election, and a message queue, all backed by PostgreSQL.

## Lock Types

| Type | Mechanism | Fencing Token | Auto-Expiry | Holds Connection | Custom Table |
|------|-----------|:---:|:---:|:---:|:---:|
| `advisory` | PostgreSQL advisory locks | - | - | + | - |
| `session` | Table-based, `SELECT ... FOR UPDATE` | + | - | + | + |
| `lease` | Table-based, timestamp TTL + auto-renew | + | + | - | + |

- **Advisory** - leverages PostgreSQL's built-in advisory locks. No table or schema setup required. Holds a database connection for the duration of the lock. Released automatically on disconnect. Simple and lightweight, but provides no fencing tokens, so it can't protect against stale holders writing to external systems.

- **Session** - uses a dedicated table with `SELECT ... FOR UPDATE` to hold the lock within an open transaction. Issues monotonically increasing fencing tokens via a per-lock PostgreSQL sequence (`nextval`) on each acquisition. Because `nextval` is non-transactional, tokens are visible to other instances immediately - even while the lock-holding transaction is still open. This means `isSuperseded()` returns accurate results at all times. Holds a connection for the duration of the lock - if the process crashes, the connection is closed and the lock is released.

- **Lease** - does not hold a connection or transaction. Acquires the lock by writing a timestamp to a table and releases the connection immediately. The lock is held purely via a TTL (`expires_at`) - if a holder crashes, the lock automatically becomes available after the lease duration. An optional auto-renew thread extends the lease periodically to prevent expiry during long-running work. Supports a quiet period to enforce a minimum gap between consecutive acquisitions. Best suited for long-running tasks where occupying a connection pool slot is not acceptable.

## Naming Rules

Lock, queue, and leader election names must be **1–49 characters**, containing only lowercase letters, digits, hyphens, and underscores. The first character must be alphanumeric.

Valid: `my-lock`, `order_processor`, `import-job-3`
Invalid: `My-Lock`, `my lock`, `-leading-hyphen`

## Thread Safety

Lock instances are **not thread-safe**. Each instance should be confined to a single thread. If multiple threads need to compete for the same lock, each thread should create its own instance via `LockFactory.forName`:

```java
LockFactory<FencedLock> factory = Fencepost.Locks.session(dataSource).build();

// correct - each thread gets its own instance
executor.submit(() -> factory.forName("my-lock").runLocked(token -> { /* ... */ }));
executor.submit(() -> factory.forName("my-lock").runLocked(token -> { /* ... */ }));

// wrong - sharing one instance across threads
FencedLock lock = factory.forName("my-lock");
executor.submit(() -> lock.runLocked(token -> { /* ... */ }));
executor.submit(() -> lock.runLocked(token -> { /* ... */ }));
```

This applies to all lock types (`advisory`, `session`, `lease`). The `LockFactory` itself is thread-safe and can be shared freely.

## Table Setup

Session and lease locks require a single table. Advisory locks don't need any setup.

```sql
CREATE TABLE fencepost_locks (
    lock_name   TEXT PRIMARY KEY,
    lock_type   TEXT NOT NULL,
    token       BIGINT NOT NULL DEFAULT 0,
    locked_by   TEXT,
    locked_at   TIMESTAMP WITH TIME ZONE,
    expires_at  TIMESTAMP WITH TIME ZONE
);
```

The table name defaults to `fencepost_locks` but can be customized via `.tableName("my_locks")` on the builder.

Session locks allocate fencing tokens from per-lock PostgreSQL sequences (named `fencepost_st_<lock_name>`)
that Fencepost creates automatically on first acquisition. These sequences are declared with `CACHE 1` -
a larger cache hands each session a private block of pre-allocated values, which would break fencing-token
monotonicity across concurrent holders - so you never need to create or manage them yourself.

> **Use session locks for a bounded, stable set of names.** Each distinct session-lock name creates a
> dedicated sequence that is never dropped, so dynamically generated, high-cardinality names (e.g.
> `order-<uuid>`, one lock per entity) accumulate sequences in the PostgreSQL catalog indefinitely -
> leading to `pg_class` bloat and autovacuum pressure on the catalog. For high-cardinality or
> dynamically-named locks, prefer **lease** locks: they need no per-lock sequence (the fencing token
> lives in the lock row), so the catalog footprint stays constant regardless of how many names you use.

## Examples

### Advisory Lock

```java
LockFactory<Lock> fencepost = Fencepost.Locks.advisory(dataSource).build();
Lock lock = fencepost.forName("my-resource");

lock.lock();                          // blocking
lock.lock(Duration.ofSeconds(5));     // blocking with timeout
lock.tryLock();                       // non-blocking

// convenience wrapper
lock.runLocked(() -> { /* critical section */ });
```

### Session Lock

```java
LockFactory<FencedLock> fencepost = Fencepost.Locks.session(dataSource).build();
FencedLock lock = fencepost.forName("my-resource");

// fencing token protects against stale writes
lock.runLocked(token -> {
    externalStore.write(data, token.value());
});
```

### Lease Lock

```java
LockFactory<RenewableLock> fencepost = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
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

### Leader Election

Use leader election when you want one of N instances to *pick up* a piece of work and *keep doing it*, with automatic failover when the leader dies. It's built on top of `leaseLock` - a sticky single-leader primitive, not per-iteration mutual exclusion (use `leaseLock` directly for that).

```java
LeaderElection election = Fencepost.Locks.leaderElection(dataSource, "import-job", Duration.ofSeconds(30))
    .withRenewInterval(Duration.ofSeconds(10))
    .withPollInterval(Duration.ofSeconds(5))
    .withInstanceId("worker-pod-7")               // optional, written to locked_by
    .onElected(token -> startWorker(token))       // overload: receives the FencingToken
    .onRevoked(() -> stopWorker())
    .onCallbackError(e -> log.warn("...", e))     // optional
    .build();

election.start();

// elsewhere:
if (election.isLeader()) {
    // best-effort hint - fine for idempotent or token-gated work.
    // For exclusive access to an external system, carry `token` and let
    // that system reject stale tokens (see "absolute mutual-exclusion" below).
}

// on shutdown:
election.close();   // fires onRevoked synchronously, then releases the lease
```

`onElected` and `onRevoked` are state-change callbacks - they should return quickly. Real work runs on your own thread, gated by `isLeader()`. If the leader's lease can't be renewed (DB hiccup, GC pause longer than the lease), `onRevoked` fires and the loop returns to standby; another instance takes over within roughly one lease duration.

A hung or unreachable database is detected (and `onRevoked` fired) strictly before the lease can expire, so a standby never wins while the old leader still reports `isLeader() == true`. That bound covers the renew query itself - it cannot cover an unbounded pause of the leader's JVM (long GC, machine suspend), during which the lease may lapse and `isLeader()` may briefly stay `true` after another instance is already elected. `isLeader()` is therefore a best-effort hint, not a cross-node lock: to mutually exclude access to an *external* resource, gate writes on the fencing token from `onElected`, not on `isLeader()` (see [absolute mutual-exclusion guarantees](#important-postgresql-clock-behavior) below).

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
    payload       BYTEA NOT NULL,
    type          TEXT,
    headers       JSONB,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    visible_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    picked_by     TEXT,
    attempts      INT NOT NULL DEFAULT 0,
    dead_at       TIMESTAMP WITH TIME ZONE,
    last_error    TEXT
);

CREATE INDEX idx_fencepost_queue_dequeue
    ON fencepost_queue (queue_name, visible_at, id) WHERE dead_at IS NULL;

CREATE INDEX idx_fencepost_queue_dlq
    ON fencepost_queue (queue_name) WHERE dead_at IS NOT NULL;
```

The table name defaults to `fencepost_queue` but can be customized via `.tableName("my_queue")` on the builder.

### Queue Example

```java
QueueFactory<Queue> fencepost = Fencepost.Queues.queue(dataSource)
    .visibilityTimeout(Duration.ofSeconds(30)) // required
    .build();

Queue queue = fencepost.forName("my-queue");

queue.enqueue("hello".getBytes(), "greeting.v1", Map.of());
queue.enqueue("hello".getBytes(), "greeting.v1", Map.of(), Duration.ofSeconds(10));

// enqueue with headers
queue.enqueue("{\"to\":\"user@example.com\"}".getBytes(), "send-email.v1", Map.of("priority", "high"));
queue.enqueue("{\"to\":\"user@example.com\"}".getBytes(), "send-email.v1", Map.of("priority", "high"), Duration.ofSeconds(10));

Message msg = queue.dequeue();           // blocking (LISTEN/NOTIFY)
Message msg = queue.dequeue(Duration.ofSeconds(5)); // with timeout
Optional<Message> msg = queue.tryDequeue();         // non-blocking

msg.type();       // Optional[send-email.v1]
msg.headers();    // {"priority": "high"}

// ack() deletes the message, nack() makes it visible again immediately
msg.ack();
msg.nack();
```

Each message carries a required `type` (a plain text label for routing or versioning) and optional `headers` (a `Map<String, String>` stored as JSONB).

If processing fails without calling `ack()` or `nack()`, the message becomes visible again after the visibility timeout expires, with an incremented `attempts` counter.

### Queue Publisher

When you only need to publish messages (no dequeue), use the standalone publisher:

```java
QueueFactory<QueuePublisher> fencepost = Fencepost.Queues.publisher(dataSource).build();
QueuePublisher publisher = fencepost.forName("my-queue");

publisher.publish("hello".getBytes(), "greeting.v1");
publisher.publish("hello".getBytes(), "greeting.v1", Map.of("priority", "high"));
publisher.publish("hello".getBytes(), "greeting.v1", Duration.ofSeconds(10));
```

### Transactional Publish

To publish messages within an existing JDBC transaction (e.g., alongside a business write), use the transactional publisher. The message is only visible to consumers after the transaction commits:

```java
QueuePublisher publisher = fencepost.forName("my-queue");

try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);
    // your business write
    try (var stmt = conn.prepareStatement("INSERT INTO orders (id, data) VALUES (?, ?)")) {
        stmt.setLong(1, orderId);
        stmt.setString(2, orderJson);
        stmt.executeUpdate();
    }
    // publish in the same transaction
    publisher.transactional(conn).publish(orderEvent, "order-created.v1");
    conn.commit();
}
```

### Queue Consumer

For continuous message processing, the managed consumer handles dequeue, ack/nack, concurrency, and error handling:

```java
QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "my-queue")
    .visibilityTimeout(Duration.ofSeconds(30))
    .handler(msg -> process(msg))            // auto-ack on success, nack on exception
    .concurrency(4)                          // 4 concurrent consumer threads
    .onError((msg, ex) -> log.error("failed to process message", ex))
    .build();

consumer.start();

// on shutdown:
consumer.close(); // waits for in-flight handlers to finish
```

### Retries and the dead-letter queue

A consumer that throws from its handler negatively-acknowledges the message for redelivery. Two `ConsumerBuilder` knobs control retry behavior:

- `retryDelay(Duration)` — how long a failed message stays invisible before redelivery. Defaults to **1 second** (this prevents a deterministically-failing "poison" message from hot-looping at database-round-trip speed).
- `maxDeliveries(int)` — after a message has been delivered this many times and still fails, it is **dead-lettered** instead of retried: the row is marked with `dead_at = now()` (and `last_error`) and is no longer dequeued. Delivery count is the `attempts` counter, which increments on every pick (explicit nacks and visibility-timeout redeliveries alike). Unset means unlimited retries.

Both knobs apply **only when the handler throws**. A handler that calls `msg.nack()` itself and returns normally requeues the message **immediately** (zero delay) and is never dead-lettered — `attempts` still counts those picks, but the `maxDeliveries` threshold is only evaluated when a delivery ends in a throw.

```java
QueueConsumer consumer = Fencepost.Queues.consumer(dataSource, "orders")
    .visibilityTimeout(Duration.ofMinutes(5))
    .maxDeliveries(5)                 // 5 attempts, then dead-letter
    .retryDelay(Duration.ofSeconds(2))
    .handler(msg -> process(msg))
    .build();
consumer.start();
```

Dead-lettered messages remain in the queue table with `dead_at` set; inspect, redrive, or drain them with SQL:

```sql
-- inspect
SELECT id, attempts, last_error, dead_at
FROM fencepost_queue
WHERE queue_name = 'orders' AND dead_at IS NOT NULL;

-- redrive: clear the marker AND reset the delivery budget —
-- with attempts still >= maxDeliveries, the next failure would
-- instantly dead-letter the message again
UPDATE fencepost_queue
SET dead_at = NULL, last_error = NULL, attempts = 0
WHERE queue_name = 'orders' AND id = 42;

-- drain
DELETE FROM fencepost_queue
WHERE queue_name = 'orders' AND dead_at IS NOT NULL;
```

## Important: PostgreSQL Clock Behavior

PostgreSQL's `clock_timestamp()` / `now()` relies on the system clock, which is **not monotonic** and is subject to clock skew (e.g., NTP adjustments, VM clock drift, leap second handling). This means that timestamp-based lease expiry can, in rare cases, behave unexpectedly - a lease may appear to expire early or late if the database server's clock jumps.

If your use case requires **absolute mutual-exclusion guarantees** (e.g., protecting writes to an external system), you have two options:

1. **Use fencing tokens** - the fencing token is a monotonically increasing value that lets downstream systems reject stale writes, regardless of clock behavior. Pass the token to any external resource and have that resource reject requests with a token lower than the highest it has already seen. This works with both `session` and `lease` locks.

2. **Use a `session` lock instead** - since `session` locks are held via `SELECT ... FOR UPDATE` within an open transaction, they don't depend on timestamps at all and are immune to clock skew. The trade-off is that a session lock holds a database connection for the entire duration of the lock, which may not be acceptable for long-running tasks or applications with limited connection pools.
