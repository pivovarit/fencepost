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

## Important: PostgreSQL Clock Behavior

PostgreSQL's `clock_timestamp()` / `now()` relies on the system clock, which is **not monotonic** and is subject to clock skew (e.g., NTP adjustments, VM clock drift, leap second handling). This means that timestamp-based lease expiry can, in rare cases, behave unexpectedly - a lease may appear to expire early or late if the database server's clock jumps.

If your use case requires **absolute mutual-exclusion guarantees** (e.g., protecting writes to an external system), you have two options:

1. **Use fencing tokens** - the fencing token is a monotonically increasing value that lets downstream systems reject stale writes, regardless of clock behavior. Pass the token to any external resource and have that resource reject requests with a token lower than the highest it has already seen. This works with both `session` and `lease` locks.

2. **Use a `session` lock instead** - since `session` locks are held via `SELECT ... FOR UPDATE` within an open transaction, they don't depend on timestamps at all and are immune to clock skew. The trade-off is that a session lock holds a database connection for the entire duration of the lock, which may not be acceptable for long-running tasks or applications with limited connection pools.

