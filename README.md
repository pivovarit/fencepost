# fencepost

Distributed concurrency toolkit for Java + PostgreSQL.

Zero dependencies beyond `java.sql`. Requires Java 11+.

**Under construction.**

## Lock Types

Fencepost provides three lock strategies, all backed by PostgreSQL:

| Type | Mechanism | Fencing Token | Auto-Expiry | Holds Connection |
|------|-----------|:---:|:---:|:---:|
| `advisory` | PostgreSQL advisory locks | - | - | + |
| `connection` | Table-based, `SELECT ... FOR UPDATE` | + | - | + |
| `lease` | Table-based, timestamp TTL + heartbeat | + | + | - |

- **Advisory** - leverages PostgreSQL's built-in advisory locks. No table or schema setup required. Holds a database connection for the duration of the lock. Released automatically on disconnect. Simple and lightweight, but provides no fencing tokens, so it can't protect against stale holders writing to external systems.

- **Connection** - uses a dedicated table with `SELECT ... FOR UPDATE` to hold the lock within an open transaction. Issues monotonically increasing fencing tokens on each acquisition. The token lets downstream systems reject writes from holders that have been superseded. Holds a connection for the duration of the lock - if the process crashes, the connection is closed and the lock is released.

- **Lease** - does not hold a connection or transaction. Acquires the lock by writing a timestamp to a table and releases the connection immediately. The lock is held purely via a TTL (`expires_at`) - if a holder crashes, the lock automatically becomes available after the lease duration. An optional heartbeat thread renews the lease periodically to prevent expiry during long-running work. Supports a quiet period to enforce a minimum gap between consecutive acquisitions. Best suited for long-running tasks where occupying a connection pool slot is not acceptable.

