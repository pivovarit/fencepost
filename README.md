# fencepost

Distributed concurrency toolkit for Java + PostgreSQL.

Zero dependencies beyond `java.sql`. Requires Java 11+.

**Under construction.**

## Lock Types

Fencepost provides three lock strategies, all backed by PostgreSQL:

| Type | Mechanism | Fencing Token | Auto-Expiry | Holds Connection |
|------|-----------|:---:|:---:|:---:|
| `advisory` | PostgreSQL advisory locks | - | - | + |
| `session` | Table-based, `SELECT ... FOR UPDATE` | + | - | + |
| `lease` | Table-based, timestamp TTL + heartbeat | + | + | - |

- **Advisory** - leverages PostgreSQL's built-in advisory locks. No table or schema setup required. Holds a database connection for the duration of the lock. Released automatically on disconnect. Simple and lightweight, but provides no fencing tokens, so it can't protect against stale holders writing to external systems.

- **Session** - uses a dedicated table with `SELECT ... FOR UPDATE` to hold the lock within an open transaction. Issues monotonically increasing fencing tokens on each acquisition. The token lets downstream systems reject writes from holders that have been superseded. Holds a connection for the duration of the lock - if the process crashes, the connection is closed and the lock is released.

- **Lease** - does not hold a connection or transaction. Acquires the lock by writing a timestamp to a table and releases the connection immediately. The lock is held purely via a TTL (`expires_at`) - if a holder crashes, the lock automatically becomes available after the lease duration. An optional heartbeat thread renews the lease periodically to prevent expiry during long-running work. Supports a quiet period to enforce a minimum gap between consecutive acquisitions. Best suited for long-running tasks where occupying a connection pool slot is not acceptable.

## Examples

### Advisory Lock

The simplest option — no table setup required. Holds a database connection for the duration of the lock:

```java
Fencepost<Lock> fencepost = Fencepost.advisoryLock(dataSource).build();
Lock lock = fencepost.forName("my-resource");

// option 1: explicit lock/unlock
lock.lock();
try {
    // critical section
} finally {
    lock.unlock();
}

// option 2: with timeout
lock.lock(Duration.ofSeconds(5));
try {
    // critical section
} finally {
    lock.unlock();
}

// option 3: non-blocking
if (lock.tryLock()) {
    try {
        // critical section
    } finally {
        lock.unlock();
    }
}

// option 4: convenience wrapper
lock.withLock(() -> {
    // critical section
});
```

### Session Lock

Table-based lock held via `SELECT ... FOR UPDATE`. Issues fencing tokens to protect against stale holders:

```java
Fencepost<FencedLock> fencepost = Fencepost.sessionLock(dataSource)
    .tableName("my_locks") // optional, defaults to "fencepost_locks"
    .build();

FencedLock lock = fencepost.forName("my-resource");

// acquire with fencing token
FencingToken token = lock.lock();
try {
    // pass token to external systems to reject stale writes
    externalStore.write(data, token.value());
} finally {
    lock.unlock();
}

// non-blocking with fencing token
Optional<FencingToken> maybeToken = lock.tryLock();
maybeToken.ifPresent(t -> {
    try {
        externalStore.write(data, t.value());
    } finally {
        lock.unlock();
    }
});

// convenience wrapper with token access
lock.withLock(token -> {
    externalStore.write(data, token.value());
});
```

### Lease Lock

Timestamp-based lock that releases the connection immediately. Best for long-running tasks:

```java
Fencepost<RenewableLock> fencepost = Fencepost.leaseLock(dataSource, Duration.ofSeconds(30))
    .tableName("my_locks")                  // optional
    .withHeartbeat(Duration.ofSeconds(10))   // auto-renew before expiry
    .withQuietPeriod(Duration.ofSeconds(5))  // min gap between acquisitions
    .onHeartbeatFailure(e -> log.error("heartbeat failed", e))
    .build();

RenewableLock lock = fencepost.forName("my-resource");

// heartbeat thread keeps the lease alive automatically
FencingToken token = lock.lock();
try {
    longRunningTask(token);
} finally {
    lock.unlock();
}

// manual renewal (when not using heartbeat)
lock.renew(Duration.ofSeconds(30));
```

## Important: PostgreSQL Clock Behavior

PostgreSQL's `clock_timestamp()` / `now()` relies on the system clock, which is **not monotonic** and is subject to clock skew (e.g., NTP adjustments, VM clock drift, leap second handling). This means that timestamp-based lease expiry can, in rare cases, behave unexpectedly - a lease may appear to expire early or late if the database server's clock jumps.

If your use case requires **absolute mutual-exclusion guarantees** (e.g., protecting writes to an external system), you have two options:

1. **Use fencing tokens** - the fencing token is a monotonically increasing value that lets downstream systems reject stale writes, regardless of clock behavior. Pass the token to any external resource and have that resource reject requests with a token lower than the highest it has already seen. This works with both `session` and `lease` locks.

2. **Use a `session` lock instead** - since `session` locks are held via `SELECT ... FOR UPDATE` within an open transaction, they don't depend on timestamps at all and are immune to clock skew. The trade-off is that a session lock holds a database connection for the entire duration of the lock, which may not be acceptable for long-running tasks or applications with limited connection pools.

