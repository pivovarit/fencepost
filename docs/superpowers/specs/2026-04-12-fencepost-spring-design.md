# fencepost-spring: Spring Integration Module

## Overview

A separate Maven module (`fencepost-spring`) that provides Spring Boot auto-configuration and AOP-based distributed lock interception for scheduled tasks. Brings ShedLock-like ergonomics to fencepost while exposing fencepost's richer lock primitives (fencing tokens, advisory locks, auto-renew).

The core library remains untouched — zero new dependencies, no API changes.

## Module Structure

The project becomes a multi-module Maven build:

```
fencepost/
├── pom.xml                    (parent POM)
├── fencepost-core/
│   ├── pom.xml                (current fencepost, relocated)
│   └── src/
└── fencepost-spring/
    ├── pom.xml
    └── src/
```

### Dependencies for `fencepost-spring`

| Dependency | Scope |
|---|---|
| `fencepost-core` | compile |
| `spring-context` (includes spring-aop) | provided |
| `spring-boot-autoconfigure` | provided |

Java version: 17+ (Spring Boot 3.x requirement). Core module stays at Java 11+.

## Annotation: `@FencepostLock`

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FencepostLock {
    String name();                              // lock name, supports ${...} placeholders
    String lockAtMostFor() default "";           // e.g. "10m", "PT10M" — falls back to fencepost.default-lock-at-most-for
    String lockAtLeastFor() default "PT0S";     // clock skew protection, lease locks only
    LockType type() default LockType.LEASE;     // LEASE, ADVISORY, SESSION
}
```

### `LockType` enum

```java
public enum LockType {
    LEASE,      // RenewableLock — default, releases connection immediately, auto-expires
    ADVISORY,   // AdvisoryLock — lightweight, connection-scoped, auto-releases on connection death
    SESSION     // FencedLock — held via open transaction, provides fencing tokens
}
```

### Duration parsing

Accepts both shorthand (`10m`, `30s`, `2h`) and ISO-8601 (`PT10M`, `PT30S`) via Spring's `DurationStyle`.

### Placeholder resolution

The `name` field supports Spring `${property}` placeholders via `StringValueResolver`. No SpEL support.

### Fencing token injection

When an annotated method declares a `FencingToken` parameter, the interceptor passes the acquired token. Valid for `LEASE` and `SESSION` types only. If a method declares a `FencingToken` parameter with `LockType.ADVISORY`, the interceptor throws `IllegalStateException` at startup (fail-fast).

```java
@Scheduled(cron = "0 */5 * * * *")
@FencepostLock(name = "inventory-sync", lockAtMostFor = "10m")
public void sync(FencingToken token) {
    // token is injected by the interceptor
    externalService.update(data, token.value());
}
```

## AOP Interceptor

### `FencepostLockAdvisor`

Extends `AbstractPointcutAdvisor`:
- **Pointcut:** `AnnotationMatchingPointcut` targeting methods annotated with `@FencepostLock`
- **Advice:** `FencepostLockInterceptor` (implements `MethodInterceptor`)

### `FencepostLockInterceptor` flow

```
1. Extract @FencepostLock annotation from target method
2. Resolve lock name (placeholder substitution)
3. Parse lockAtMostFor / lockAtLeastFor durations
4. Based on LockType, acquire the appropriate lock via tryLock():
   - LEASE:    fencepost.lease(name).tryLock() with lease duration = lockAtMostFor
   - ADVISORY: fencepost.advisory(name).tryLock()
   - SESSION:  fencepost.session(name).tryLock()
5. If lock acquired:
     - If method has FencingToken parameter -> pass token
     - invocation.proceed()
     - finally: unlock
       - For LEASE: set expires_at to max(now, acquiredAt + lockAtLeastFor)
       - For ADVISORY/SESSION: unlock immediately (lockAtLeastFor ignored)
6. If lock not acquired:
     - Skip execution, return null
     - Log at DEBUG level that execution was skipped
```

### Key semantics

- **Non-blocking:** Always uses `tryLock()`, never `lock()`. If the lock is held by another node, execution is skipped silently (DEBUG log).
- **`lockAtMostFor` (lease only):** Maps to the lease duration. Acts as a crash safety net — if the node dies without releasing, the lock auto-expires.
- **`lockAtLeastFor` (lease only):** On unlock, `expires_at` is set to `max(now, acquiredAt + lockAtLeastFor)`. Prevents rapid re-acquisition across nodes due to clock skew. Not supported for advisory/session locks (documented, not enforced at annotation level).
- **`lockAtMostFor` for advisory/session:** Not directly applicable (advisory expires on connection death, session on transaction end). The interceptor unlocks after the method completes regardless.

## Auto-Configuration

### `FencepostAutoConfiguration`

Registered via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

```java
@Configuration
@ConditionalOnClass(Fencepost.class)
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(FencepostProperties.class)
public class FencepostAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Fencepost fencepost(DataSource dataSource, FencepostProperties props) {
        // Build Fencepost factory from DataSource + properties
    }

    @Bean
    public FencepostLockAdvisor fencepostLockAdvisor(Fencepost fencepost,
                                                      FencepostProperties props) {
        // Create the AOP advisor with interceptor wired in
    }
}
```

**Backs off when:** user declares their own `Fencepost` bean (`@ConditionalOnMissingBean`). The advisor activates whenever `Fencepost` is on the classpath.

### `FencepostProperties`

```yaml
fencepost:
  table-name: fencepost_locks          # default
  queue-table-name: fencepost_queue    # default
  default-lock-at-most-for: 10m       # fallback when @FencepostLock.lockAtMostFor is empty; if both are empty, startup fails
```

## Testing Strategy

### Unit tests

- `FencepostLockInterceptor` tested in isolation with a mock `Fencepost` factory:
  - Lock acquired -> method proceeds
  - Lock not acquired -> method skipped, returns null
  - Fencing token injected into method parameter
  - `IllegalStateException` on `FencingToken` param with `ADVISORY` type (fail-fast at startup)
- Annotation parsing and duration resolution
- Placeholder resolution in lock names

### Integration tests

- Spring Boot test with Testcontainers PostgreSQL (same pattern as core module):
  - Full flow: auto-config creates beans, `@Scheduled` + `@FencepostLock` method only runs once under concurrent invocation
  - Each `LockType` works end-to-end
  - Fencing token injection with `LEASE` and `SESSION`
  - Auto-config backs off when user declares their own `Fencepost` bean

## Out of Scope

- **Auto-DDL / schema management** — users handle migrations themselves
- **Spring Boot Actuator integration** — no health indicators or metrics
- **Reactive support** — no R2DBC or reactive scheduler interception
- **Multiple DataSource routing** — declare your own `Fencepost` bean to customize
- **SpEL expressions** in lock names — only `${property}` placeholders
- **`lockAtLeastFor` for advisory/session locks** — only lease locks support it
- **Non-Spring usage** — the core library already covers that

Any of these can be added later without breaking changes.
