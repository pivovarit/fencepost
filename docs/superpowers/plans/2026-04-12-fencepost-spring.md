# fencepost-spring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `fencepost-spring` module that provides Spring Boot auto-configuration and AOP-based `@FencepostLock` interception for distributed lock acquisition around scheduled (or any) methods.

**Architecture:** A Spring AOP `AbstractPointcutAdvisor` intercepts `@FencepostLock`-annotated methods, acquires the appropriate lock type via the core `Fencepost` API (`tryLock()`), and skips execution if the lock is held elsewhere. A Spring Boot auto-configuration class wires up the `Fencepost` factory bean and advisor from the application's `DataSource`.

**Tech Stack:** Java 17, Spring Boot 3.x (spring-context, spring-aop, spring-boot-autoconfigure), fencepost-core, JUnit 5, Testcontainers PostgreSQL, AssertJ.

---

## File Structure

```
fencepost/
├── pom.xml                                          (MODIFY — convert to parent POM)
├── fencepost-core/
│   ├── pom.xml                                      (CREATE — child POM, inherits from parent)
│   └── src/                                         (MOVE — existing src/ relocated here)
└── fencepost-spring/
    ├── pom.xml                                      (CREATE — child POM with spring deps)
    └── src/
        ├── main/java/com/pivovarit/fencepost/spring/
        │   ├── FencepostLock.java                   (CREATE — annotation)
        │   ├── LockType.java                        (CREATE — enum)
        │   ├── FencepostLockAdvisor.java             (CREATE — AOP advisor)
        │   ├── FencepostLockInterceptor.java         (CREATE — method interceptor)
        │   ├── FencepostProperties.java              (CREATE — config properties)
        │   └── FencepostAutoConfiguration.java       (CREATE — auto-config)
        ├── main/resources/META-INF/spring/
        │   └── org.springframework.boot.autoconfigure.AutoConfiguration.imports  (CREATE)
        └── test/java/com/pivovarit/fencepost/spring/
            ├── FencepostLockInterceptorTest.java     (CREATE — unit tests)
            └── FencepostSpringIntegrationTest.java   (CREATE — integration tests)
```

---

### Task 1: Convert to Multi-Module Maven Build

**Files:**
- Modify: `pom.xml` (root)
- Create: `fencepost-core/pom.xml`
- Move: `src/` → `fencepost-core/src/`

This task restructures the project into a multi-module layout without changing any source code.

- [ ] **Step 1: Move existing source tree into fencepost-core**

```bash
cd /Users/pivovarit/Dev/pivovarit/fencepost
mkdir fencepost-core
git mv src fencepost-core/src
```

- [ ] **Step 2: Create fencepost-core/pom.xml**

Create `fencepost-core/pom.xml` with the following content. This inherits from the parent and carries all the existing dependencies and plugins:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.pivovarit</groupId>
        <artifactId>fencepost-parent</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </parent>

    <artifactId>fencepost</artifactId>
    <name>fencepost</name>
    <description>Distributed concurrency toolkit for Java + PostgreSQL</description>

    <properties>
        <maven.compiler.release>11</maven.compiler.release>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>provided</scope>
        </dependency>

        <!-- test -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <release>${maven.compiler.release}</release>
                    <compilerArgs>
                        <arg>-Xlint:all</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Convert root pom.xml to parent POM**

Replace the root `pom.xml` with a parent POM. Move dependency versions to `<dependencyManagement>`, move plugin versions to `<pluginManagement>`, change packaging to `pom`, change artifactId to `fencepost-parent`, and add `<modules>`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.pivovarit</groupId>
    <artifactId>fencepost-parent</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>fencepost-parent</name>
    <description>Distributed concurrency toolkit for Java + PostgreSQL</description>
    <url>https://github.com/pivovarit/fencepost</url>

    <modules>
        <module>fencepost-core</module>
    </modules>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <slf4j.version>2.0.17</slf4j.version>
        <postgresql.version>42.7.10</postgresql.version>
        <junit.version>5.11.3</junit.version>
        <assertj.version>3.27.7</assertj.version>
        <awaitility.version>4.3.0</awaitility.version>
        <testcontainers.version>2.0.4</testcontainers.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>com.pivovarit</groupId>
                <artifactId>fencepost</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
            <dependency>
                <groupId>org.postgresql</groupId>
                <artifactId>postgresql</artifactId>
                <version>${postgresql.version}</version>
            </dependency>
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
            </dependency>
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>${assertj.version}</version>
            </dependency>
            <dependency>
                <groupId>org.awaitility</groupId>
                <artifactId>awaitility</artifactId>
                <version>${awaitility.version}</version>
            </dependency>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>postgresql</artifactId>
                <version>${testcontainers.version}</version>
            </dependency>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${testcontainers.version}</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-simple</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.15.0</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>3.5.5</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-source-plugin</artifactId>
                    <version>3.4.0</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-javadoc-plugin</artifactId>
                    <version>3.12.0</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>

    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
    </licenses>

    <scm>
        <connection>scm:git:git://github.com/pivovarit/fencepost.git</connection>
        <developerConnection>scm:git:ssh://github.com:pivovarit/fencepost.git</developerConnection>
        <url>https://github.com/pivovarit/fencepost</url>
    </scm>

    <developers>
        <developer>
            <name>Grzegorz Piwowarek</name>
            <email>gpiwowarek@gmail.com</email>
        </developer>
    </developers>

    <profiles>
        <profile>
            <id>sonatype-oss-release</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-gpg-plugin</artifactId>
                        <version>3.2.8</version>
                        <executions>
                            <execution>
                                <id>sign-artifacts</id>
                                <phase>verify</phase>
                                <goals><goal>sign</goal></goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.sonatype.central</groupId>
                        <artifactId>central-publishing-maven-plugin</artifactId>
                        <version>0.10.0</version>
                        <extensions>true</extensions>
                        <configuration>
                            <publishingServerId>central</publishingServerId>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>
</project>
```

- [ ] **Step 4: Verify the core module builds and tests pass**

```bash
cd /Users/pivovarit/Dev/pivovarit/fencepost
mvn clean verify
```

Expected: BUILD SUCCESS. All existing tests pass. No compilation errors.

- [ ] **Step 5: Commit**

```bash
git add pom.xml fencepost-core/
git commit -m "refactor: convert to multi-module Maven build

Move existing sources into fencepost-core module.
Root pom.xml becomes parent POM with dependency/plugin management.
Core module retains artifactId 'fencepost' for backward compatibility."
```

---

### Task 2: Create fencepost-spring Module Skeleton

**Files:**
- Modify: `pom.xml` (root — add module)
- Create: `fencepost-spring/pom.xml`

- [ ] **Step 1: Add fencepost-spring to parent modules**

In root `pom.xml`, add `fencepost-spring` to the `<modules>` section:

```xml
<modules>
    <module>fencepost-core</module>
    <module>fencepost-spring</module>
</modules>
```

- [ ] **Step 2: Create fencepost-spring/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.pivovarit</groupId>
        <artifactId>fencepost-parent</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </parent>

    <artifactId>fencepost-spring</artifactId>
    <name>fencepost-spring</name>
    <description>Spring Boot integration for fencepost distributed locks</description>

    <properties>
        <maven.compiler.release>17</maven.compiler.release>
        <spring-boot.version>3.4.5</spring-boot.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.pivovarit</groupId>
            <artifactId>fencepost</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-autoconfigure</artifactId>
            <version>${spring-boot.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-aop</artifactId>
            <version>6.2.6</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <!-- test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <version>${spring-boot.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <release>${maven.compiler.release}</release>
                    <compilerArgs>
                        <arg>-Xlint:all</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Create source directories**

```bash
mkdir -p fencepost-spring/src/main/java/com/pivovarit/fencepost/spring
mkdir -p fencepost-spring/src/main/resources/META-INF/spring
mkdir -p fencepost-spring/src/test/java/com/pivovarit/fencepost/spring
```

- [ ] **Step 4: Verify the build succeeds**

```bash
mvn clean verify
```

Expected: BUILD SUCCESS (fencepost-spring compiles with no sources, no errors).

- [ ] **Step 5: Commit**

```bash
git add pom.xml fencepost-spring/
git commit -m "feat: add fencepost-spring module skeleton

Empty module with Spring Boot 3.x and spring-aop as provided dependencies.
Java 17 target (Spring Boot 3.x requirement)."
```

---

### Task 3: Annotation and LockType Enum

**Files:**
- Create: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/LockType.java`
- Create: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLock.java`
- Test: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockAnnotationTest.java`

- [ ] **Step 1: Write test for annotation presence and defaults**

Create `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockAnnotationTest.java`:

```java
package com.pivovarit.fencepost.spring;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class FencepostLockAnnotationTest {

    @Test
    void shouldHaveCorrectDefaults() throws NoSuchMethodException {
        Method method = AnnotatedSample.class.getDeclaredMethod("withDefaults");
        FencepostLock annotation = method.getAnnotation(FencepostLock.class);

        assertThat(annotation.name()).isEqualTo("test-lock");
        assertThat(annotation.lockAtMostFor()).isEqualTo("10m");
        assertThat(annotation.lockAtLeastFor()).isEqualTo("PT0S");
        assertThat(annotation.type()).isEqualTo(LockType.LEASE);
    }

    @Test
    void shouldAllowOverridingAllAttributes() throws NoSuchMethodException {
        Method method = AnnotatedSample.class.getDeclaredMethod("withOverrides");
        FencepostLock annotation = method.getAnnotation(FencepostLock.class);

        assertThat(annotation.name()).isEqualTo("custom-lock");
        assertThat(annotation.lockAtMostFor()).isEqualTo("PT30M");
        assertThat(annotation.lockAtLeastFor()).isEqualTo("5s");
        assertThat(annotation.type()).isEqualTo(LockType.ADVISORY);
    }

    static class AnnotatedSample {
        @FencepostLock(name = "test-lock", lockAtMostFor = "10m")
        void withDefaults() {}

        @FencepostLock(name = "custom-lock", lockAtMostFor = "PT30M", lockAtLeastFor = "5s", type = LockType.ADVISORY)
        void withOverrides() {}
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/pivovarit/Dev/pivovarit/fencepost
mvn test -pl fencepost-spring -Dtest=FencepostLockAnnotationTest
```

Expected: COMPILATION ERROR — `FencepostLock` and `LockType` do not exist.

- [ ] **Step 3: Create LockType enum**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/LockType.java`:

```java
package com.pivovarit.fencepost.spring;

/**
 * Lock strategy to use for {@link FencepostLock} acquisition.
 */
public enum LockType {
    /** Timestamp-based lease lock. Releases connection immediately. Auto-expires. */
    LEASE,
    /** PostgreSQL advisory lock. Lightweight, auto-releases on connection death. */
    ADVISORY,
    /** Row-level lock held via open transaction. Provides fencing tokens. */
    SESSION
}
```

- [ ] **Step 4: Create FencepostLock annotation**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLock.java`:

```java
package com.pivovarit.fencepost.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Acquires a distributed lock before method execution.
 * If the lock is already held, execution is skipped.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FencepostLock {

    /** Lock name. Supports Spring {@code ${...}} property placeholders. */
    String name();

    /**
     * Maximum duration to hold the lock (crash safety).
     * Accepts shorthand ({@code "10m"}, {@code "30s"}) or ISO-8601 ({@code "PT10M"}).
     * Falls back to {@code fencepost.default-lock-at-most-for} property if empty.
     */
    String lockAtMostFor() default "";

    /**
     * Minimum duration to hold the lock after execution (clock skew protection).
     * Only supported for {@link LockType#LEASE}. Ignored for other lock types.
     */
    String lockAtLeastFor() default "PT0S";

    /** Lock strategy. Defaults to {@link LockType#LEASE}. */
    LockType type() default LockType.LEASE;
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockAnnotationTest
```

Expected: 2 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add fencepost-spring/src/
git commit -m "feat(spring): add @FencepostLock annotation and LockType enum"
```

---

### Task 4: FencepostLockInterceptor

**Files:**
- Create: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLockInterceptor.java`
- Test: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockInterceptorTest.java`

This is the core logic — the `MethodInterceptor` that acquires locks and delegates to the target method.

**Key design:** Different `@FencepostLock` methods can have different `lockAtMostFor` / `lockAtLeastFor` values. Since the core `Fencepost` builders bake these durations into the `Factory` at build time, the interceptor caches factories keyed by their effective configuration (lock type + durations). It takes `DataSource` + `tableName` rather than pre-built factories.

- [ ] **Step 1: Write test for lock-acquired-and-method-proceeds**

Create `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockInterceptorTest.java`:

```java
package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.lock.FencingToken;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@Testcontainers
class FencepostLockInterceptorTest {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17");

    static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        return ds;
    }

    static void createTable(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                "DROP TABLE IF EXISTS fencepost_locks; " +
                "CREATE TABLE fencepost_locks (" +
                "  lock_name TEXT PRIMARY KEY," +
                "  token BIGINT NOT NULL DEFAULT 0," +
                "  locked_by TEXT," +
                "  locked_at TIMESTAMP WITH TIME ZONE," +
                "  expires_at TIMESTAMP WITH TIME ZONE" +
                ")"
            );
        }
    }

    @Test
    void shouldProceedWhenLeaseLockAcquired() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        MethodInvocation invocation = mockInvocation("leaseLocked", "expected-result");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("expected-result");
        verify(invocation).proceed();
    }

    @Test
    void shouldInjectFencingTokenParameter() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        Method method = SampleMethods.class.getDeclaredMethod("leaseLockedWithToken", FencingToken.class);
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[]{null});
        when(invocation.proceed()).thenReturn("ok");

        interceptor.invoke(invocation);

        verify(invocation).proceed();
        assertThat(invocation.getArguments()[0]).isNotNull();
        assertThat(((FencingToken) invocation.getArguments()[0]).value()).isPositive();
    }

    @Test
    void shouldAcquireAdvisoryLockWhenTypeIsAdvisory() throws Throwable {
        DataSource ds = dataSource();
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        MethodInvocation invocation = mockInvocation("advisoryLocked", "result");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("result");
        verify(invocation).proceed();
    }

    @Test
    void shouldAcquireSessionLockWhenTypeIsSession() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        MethodInvocation invocation = mockInvocation("sessionLocked", "result");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("result");
        verify(invocation).proceed();
    }

    private MethodInvocation mockInvocation(String methodName, Object returnValue) throws Throwable {
        Method method = SampleMethods.class.getDeclaredMethod(methodName);
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[0]);
        when(invocation.proceed()).thenReturn(returnValue);
        return invocation;
    }

    static class SampleMethods {
        @FencepostLock(name = "test-lock", lockAtMostFor = "10m")
        Object leaseLocked() { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m")
        Object leaseLockedWithToken(FencingToken token) { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m", type = LockType.ADVISORY)
        Object advisoryLocked() { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m", type = LockType.SESSION)
        Object sessionLocked() { return null; }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockInterceptorTest
```

Expected: COMPILATION ERROR — `FencepostLockInterceptor` does not exist.

- [ ] **Step 3: Implement FencepostLockInterceptor**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLockInterceptor.java`:

```java
package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.Fencepost;
import com.pivovarit.fencepost.Factory;
import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

class FencepostLockInterceptor implements MethodInterceptor {

    private static final Logger log = LoggerFactory.getLogger(FencepostLockInterceptor.class);

    private final DataSource dataSource;
    private final String tableName;
    private final Duration defaultLockAtMostFor;
    private final UnaryOperator<String> placeholderResolver;

    // Factories cached by effective config — advisory and session don't vary by duration
    private volatile Factory<AdvisoryLock> advisoryFactory;
    private volatile Factory<FencedLock> sessionFactory;
    private final ConcurrentHashMap<LeaseConfig, Factory<RenewableLock>> leaseFactories = new ConcurrentHashMap<>();

    FencepostLockInterceptor(DataSource dataSource,
                             String tableName,
                             Duration defaultLockAtMostFor,
                             UnaryOperator<String> placeholderResolver) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.defaultLockAtMostFor = defaultLockAtMostFor;
        this.placeholderResolver = placeholderResolver;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        Method method = invocation.getMethod();
        FencepostLock annotation = method.getAnnotation(FencepostLock.class);

        String lockName = placeholderResolver.apply(annotation.name());
        LockType type = annotation.type();

        return switch (type) {
            case LEASE -> invokeWithLease(invocation, lockName, annotation);
            case SESSION -> invokeWithSession(invocation, lockName);
            case ADVISORY -> invokeWithAdvisory(invocation, lockName);
        };
    }

    private Object invokeWithLease(MethodInvocation invocation, String lockName, FencepostLock annotation) throws Throwable {
        Duration lockAtMostFor = resolveDuration(annotation.lockAtMostFor(), defaultLockAtMostFor);
        Duration lockAtLeastFor = parseDuration(annotation.lockAtLeastFor());

        var config = new LeaseConfig(lockAtMostFor, lockAtLeastFor);
        Factory<RenewableLock> factory = leaseFactories.computeIfAbsent(config, c -> {
            var builder = Fencepost.leaseLock(dataSource, c.lockAtMostFor).tableName(tableName);
            if (c.lockAtLeastFor != null && !c.lockAtLeastFor.isZero()) {
                builder.withQuietPeriod(c.lockAtLeastFor);
            }
            return builder.build();
        });

        RenewableLock lock = factory.forName(lockName);
        Optional<FencingToken> token = lock.tryLock();
        if (token.isEmpty()) {
            log.debug("Skipping execution of {}, lock '{}' is held", invocation.getMethod().getName(), lockName);
            return null;
        }
        try {
            injectFencingToken(invocation, token.get());
            return invocation.proceed();
        } finally {
            lock.unlock();
        }
    }

    private Object invokeWithSession(MethodInvocation invocation, String lockName) throws Throwable {
        if (sessionFactory == null) {
            sessionFactory = Fencepost.sessionLock(dataSource).tableName(tableName).build();
        }
        FencedLock lock = sessionFactory.forName(lockName);
        Optional<FencingToken> token = lock.tryLock();
        if (token.isEmpty()) {
            log.debug("Skipping execution of {}, lock '{}' is held", invocation.getMethod().getName(), lockName);
            return null;
        }
        try {
            injectFencingToken(invocation, token.get());
            return invocation.proceed();
        } finally {
            lock.unlock();
        }
    }

    private Object invokeWithAdvisory(MethodInvocation invocation, String lockName) throws Throwable {
        if (advisoryFactory == null) {
            advisoryFactory = Fencepost.advisoryLock(dataSource).build();
        }
        AdvisoryLock lock = advisoryFactory.forName(lockName);
        if (!lock.tryLock()) {
            log.debug("Skipping execution of {}, lock '{}' is held", invocation.getMethod().getName(), lockName);
            return null;
        }
        try {
            return invocation.proceed();
        } finally {
            lock.unlock();
        }
    }

    private void injectFencingToken(MethodInvocation invocation, FencingToken token) {
        Object[] args = invocation.getArguments();
        Class<?>[] paramTypes = invocation.getMethod().getParameterTypes();
        for (int i = 0; i < paramTypes.length; i++) {
            if (paramTypes[i] == FencingToken.class) {
                args[i] = token;
                return;
            }
        }
    }

    private Duration resolveDuration(String annotationValue, Duration fallback) {
        Duration parsed = parseDuration(annotationValue);
        if (parsed != null && !parsed.isZero()) {
            return parsed;
        }
        return fallback;
    }

    private static Duration parseDuration(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Duration.parse(value);
        } catch (Exception e) {
            return DurationParser.parse(value);
        }
    }

    private record LeaseConfig(Duration lockAtMostFor, Duration lockAtLeastFor) {}
}
```

**Key design decisions:**
- `LeaseConfig` record is used as a cache key for lease factories, since different annotation values produce different builder configurations.
- Advisory and session factories are lazily initialized (no duration parameters, so one factory each).
- `lockAtLeastFor` maps to the core library's `quietPeriod` builder parameter, which sets `expires_at = GREATEST(now(), locked_at + quietPeriod)` on unlock — exactly the ShedLock `lockAtLeastFor` semantics.

- [ ] **Step 4: Run tests to verify they pass**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockInterceptorTest
```

Expected: 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add fencepost-spring/src/
git commit -m "feat(spring): add FencepostLockInterceptor

AOP MethodInterceptor that acquires locks via tryLock() before
method execution. Supports LEASE, SESSION, and ADVISORY lock types.
Injects FencingToken into method parameters when declared."
```

---

### Task 5: FencepostLockAdvisor

**Files:**
- Create: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLockAdvisor.java`
- Test: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockAdvisorTest.java`

- [ ] **Step 1: Write test for advisor pointcut matching**

Create `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockAdvisorTest.java`:

```java
package com.pivovarit.fencepost.spring;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FencepostLockAdvisorTest {

    @Test
    void shouldMatchAnnotatedMethods() throws NoSuchMethodException {
        var advisor = new FencepostLockAdvisor(mock(FencepostLockInterceptor.class));

        Method annotated = Sample.class.getDeclaredMethod("locked");
        Method notAnnotated = Sample.class.getDeclaredMethod("notLocked");

        assertThat(advisor.getPointcut().getMethodMatcher().matches(annotated, Sample.class)).isTrue();
        assertThat(advisor.getPointcut().getMethodMatcher().matches(notAnnotated, Sample.class)).isFalse();
    }

    static class Sample {
        @FencepostLock(name = "test", lockAtMostFor = "10m")
        void locked() {}

        void notLocked() {}
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockAdvisorTest
```

Expected: COMPILATION ERROR — `FencepostLockAdvisor` does not exist.

- [ ] **Step 3: Implement FencepostLockAdvisor**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLockAdvisor.java`:

```java
package com.pivovarit.fencepost.spring;

import org.aopalliance.aop.Advice;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;

class FencepostLockAdvisor extends AbstractPointcutAdvisor {

    private final Pointcut pointcut = AnnotationMatchingPointcut.forMethodAnnotation(FencepostLock.class);
    private final FencepostLockInterceptor interceptor;

    FencepostLockAdvisor(FencepostLockInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public Pointcut getPointcut() {
        return pointcut;
    }

    @Override
    public Advice getAdvice() {
        return interceptor;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockAdvisorTest
```

Expected: 1 test PASS.

- [ ] **Step 5: Commit**

```bash
git add fencepost-spring/src/
git commit -m "feat(spring): add FencepostLockAdvisor

Spring AOP advisor that matches @FencepostLock-annotated methods
and delegates to FencepostLockInterceptor."
```

---

### Task 6: FencepostProperties and FencepostAutoConfiguration

**Files:**
- Create: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostProperties.java`
- Create: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostAutoConfiguration.java`
- Create: `fencepost-spring/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostAutoConfigurationTest.java`

- [ ] **Step 1: Write test for auto-configuration**

Create `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostAutoConfigurationTest.java`:

```java
package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.Fencepost;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class FencepostAutoConfigurationTest {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17");

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(FencepostAutoConfiguration.class));

    @Test
    void shouldCreateBeansWhenDataSourcePresent() {
        runner
            .withBean(DataSource.class, FencepostAutoConfigurationTest::dataSource)
            .run(context -> {
                assertThat(context).hasSingleBean(FencepostLockAdvisor.class);
            });
    }

    @Test
    void shouldNotCreateBeansWhenDataSourceMissing() {
        runner
            .run(context -> {
                assertThat(context).doesNotHaveBean(FencepostLockAdvisor.class);
            });
    }

    @Test
    void shouldApplyCustomTableName() {
        runner
            .withBean(DataSource.class, FencepostAutoConfigurationTest::dataSource)
            .withPropertyValues("fencepost.table-name=custom_locks")
            .run(context -> {
                assertThat(context).hasSingleBean(FencepostLockAdvisor.class);
            });
    }

    static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        return ds;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostAutoConfigurationTest
```

Expected: COMPILATION ERROR — `FencepostProperties` and `FencepostAutoConfiguration` do not exist.

- [ ] **Step 3: Create FencepostProperties**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostProperties.java`:

```java
package com.pivovarit.fencepost.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fencepost")
public class FencepostProperties {

    private String tableName = "fencepost_locks";
    private String queueTableName = "fencepost_queue";
    private String defaultLockAtMostFor;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getQueueTableName() {
        return queueTableName;
    }

    public void setQueueTableName(String queueTableName) {
        this.queueTableName = queueTableName;
    }

    public String getDefaultLockAtMostFor() {
        return defaultLockAtMostFor;
    }

    public void setDefaultLockAtMostFor(String defaultLockAtMostFor) {
        this.defaultLockAtMostFor = defaultLockAtMostFor;
    }
}
```

- [ ] **Step 4: Create FencepostAutoConfiguration**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostAutoConfiguration.java`:

```java
package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.Fencepost;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.time.Duration;

@Configuration
@ConditionalOnClass(Fencepost.class)
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(FencepostProperties.class)
public class FencepostAutoConfiguration {

    @Bean
    FencepostLockAdvisor fencepostLockAdvisor(DataSource dataSource, FencepostProperties properties) {
        Duration defaultLockAtMostFor = parseDuration(properties.getDefaultLockAtMostFor());

        var interceptor = new FencepostLockInterceptor(
            dataSource,
            properties.getTableName(),
            defaultLockAtMostFor,
            name -> name
        );

        return new FencepostLockAdvisor(interceptor);
    }

    private static Duration parseDuration(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        try {
            return Duration.parse(value);
        } catch (Exception e) {
            return DurationParser.parse(value);
        }
    }
}
```

- [ ] **Step 5: Create DurationParser utility**

Create `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/DurationParser.java`:

```java
package com.pivovarit.fencepost.spring;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses shorthand duration strings like "10m", "30s", "2h".
 */
class DurationParser {

    private static final Pattern PATTERN = Pattern.compile("^(\\d+)(ms|s|m|h)$");

    static Duration parse(String value) {
        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Cannot parse duration: " + value);
        }
        long amount = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            case "h" -> Duration.ofHours(amount);
            default -> throw new IllegalArgumentException("Unknown duration unit: " + matcher.group(2));
        };
    }
}
```

- [ ] **Step 6: Create auto-configuration registration file**

Create `fencepost-spring/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:

```
com.pivovarit.fencepost.spring.FencepostAutoConfiguration
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostAutoConfigurationTest
```

Expected: 3 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add fencepost-spring/src/
git commit -m "feat(spring): add auto-configuration and properties

FencepostAutoConfiguration creates the AOP advisor bean when
a DataSource is present. Configurable via fencepost.* properties.
Registered via Spring Boot auto-configuration imports."
```

---

### Task 7: Placeholder Resolution in Lock Names

**Files:**
- Modify: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostAutoConfiguration.java`
- Test: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockInterceptorTest.java` (add test)

- [ ] **Step 1: Add test for placeholder resolution**

Add to `FencepostLockInterceptorTest.java`:

```java
@Test
void shouldResolvePlaceholdersInLockName() throws Throwable {
    DataSource ds = dataSource();
    createTable(ds);
    var interceptor = new FencepostLockInterceptor(
        ds, "fencepost_locks", null,
        name -> name.replace("${lock.name}", "resolved-lock")
    );

    Method method = SampleMethods.class.getDeclaredMethod("placeholderLocked");
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[0]);
    when(invocation.proceed()).thenReturn("ok");

    Object result = interceptor.invoke(invocation);

    assertThat(result).isEqualTo("ok");
    verify(invocation).proceed();
}
```

Add to `SampleMethods`:

```java
@FencepostLock(name = "${lock.name}", lockAtMostFor = "10m")
Object placeholderLocked() { return null; }
```

- [ ] **Step 2: Run test to verify it passes**

The interceptor already delegates to `placeholderResolver`. This test validates the wiring.

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockInterceptorTest
```

Expected: 7 tests PASS.

- [ ] **Step 3: Wire StringValueResolver in auto-configuration**

Update `FencepostAutoConfiguration` to implement `EmbeddedValueResolverAware` and pass the resolver to the interceptor:

```java
@Configuration
@ConditionalOnClass(Fencepost.class)
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(FencepostProperties.class)
public class FencepostAutoConfiguration implements EmbeddedValueResolverAware {

    private StringValueResolver resolver;

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }

    @Bean
    FencepostLockAdvisor fencepostLockAdvisor(DataSource dataSource, FencepostProperties properties) {
        Duration defaultLockAtMostFor = parseDuration(properties.getDefaultLockAtMostFor());

        var interceptor = new FencepostLockInterceptor(
            dataSource,
            properties.getTableName(),
            defaultLockAtMostFor,
            name -> resolver != null ? resolver.resolveStringValue(name) : name
        );

        return new FencepostLockAdvisor(interceptor);
    }

    // parseDuration method unchanged
}
```

- [ ] **Step 4: Run all tests**

```bash
mvn test -pl fencepost-spring
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add fencepost-spring/src/
git commit -m "feat(spring): wire Spring placeholder resolution for lock names

Lock names in @FencepostLock support \${...} property placeholders
resolved via Spring's EmbeddedValueResolver."
```

---

### Task 8: Startup Validation

**Files:**
- Modify: `fencepost-spring/src/main/java/com/pivovarit/fencepost/spring/FencepostLockInterceptor.java`
- Test: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostLockInterceptorTest.java` (add tests)

Validate at invocation time that:
1. `lockAtMostFor` is set (either on annotation or via default property). Throw `IllegalStateException` if neither.
2. `FencingToken` parameter is not declared with `LockType.ADVISORY`. Throw `IllegalStateException`.

- [ ] **Step 1: Add test for missing lockAtMostFor**

Add to `FencepostLockInterceptorTest.java`:

```java
@Test
void shouldThrowWhenLockAtMostForMissingAndNoDefault() throws Throwable {
    DataSource ds = dataSource();
    var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

    Method method = SampleMethods.class.getDeclaredMethod("noLockAtMostFor");
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[0]);

    assertThatThrownBy(() -> interceptor.invoke(invocation))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("lockAtMostFor");
}
```

Add to `SampleMethods`:

```java
@FencepostLock(name = "test-lock")
Object noLockAtMostFor() { return null; }
```

- [ ] **Step 2: Add test for FencingToken with ADVISORY**

```java
@Test
void shouldThrowWhenFencingTokenUsedWithAdvisory() throws Throwable {
    DataSource ds = dataSource();
    var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

    Method method = SampleMethods.class.getDeclaredMethod("advisoryWithToken", FencingToken.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[]{null});

    assertThatThrownBy(() -> interceptor.invoke(invocation))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("FencingToken")
        .hasMessageContaining("ADVISORY");
}
```

Add to `SampleMethods`:

```java
@FencepostLock(name = "test-lock", lockAtMostFor = "10m", type = LockType.ADVISORY)
Object advisoryWithToken(FencingToken token) { return null; }
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockInterceptorTest
```

Expected: 2 new tests FAIL (no validation yet).

- [ ] **Step 4: Add validation to FencepostLockInterceptor.invoke()**

Add validation at the top of the existing `invoke()` method, before the `switch`:

```java
// Validate lockAtMostFor for lease locks
if (type == LockType.LEASE) {
    Duration lockAtMostFor = resolveDuration(annotation.lockAtMostFor(), defaultLockAtMostFor);
    if (lockAtMostFor == null || lockAtMostFor.isZero()) {
        throw new IllegalStateException(
            "lockAtMostFor must be specified on @FencepostLock or via fencepost.default-lock-at-most-for property for method: " + method);
    }
}

// Validate FencingToken not used with ADVISORY
if (type == LockType.ADVISORY && hasFencingTokenParameter(method)) {
    throw new IllegalStateException(
        "FencingToken parameter is not supported with LockType.ADVISORY on method: " + method);
}
```

Add the helper method:

```java
private boolean hasFencingTokenParameter(Method method) {
    for (Class<?> paramType : method.getParameterTypes()) {
        if (paramType == FencingToken.class) {
            return true;
        }
    }
    return false;
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostLockInterceptorTest
```

Expected: 9 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add fencepost-spring/src/
git commit -m "feat(spring): add startup validation for @FencepostLock

Fail fast when lockAtMostFor is missing (LEASE type) and no default
is configured. Fail fast when FencingToken parameter is used with
ADVISORY lock type."
```

---

### Task 9: Integration Tests

**Files:**
- Create: `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostSpringIntegrationTest.java`

End-to-end tests with a real Spring Boot context and Testcontainers PostgreSQL.

- [ ] **Step 1: Create integration test**

Create `fencepost-spring/src/test/java/com/pivovarit/fencepost/spring/FencepostSpringIntegrationTest.java`:

```java
package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.lock.FencingToken;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(classes = FencepostSpringIntegrationTest.TestConfig.class)
class FencepostSpringIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17");

    @Autowired
    private LockedService lockedService;

    @BeforeAll
    static void createTable() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS fencepost_locks (" +
                "  lock_name TEXT PRIMARY KEY," +
                "  token BIGINT NOT NULL DEFAULT 0," +
                "  locked_by TEXT," +
                "  locked_at TIMESTAMP WITH TIME ZONE," +
                "  expires_at TIMESTAMP WITH TIME ZONE" +
                ")"
            );
        }
    }

    @Test
    void shouldExecuteWhenLockAvailable() {
        lockedService.counter.set(0);
        lockedService.leaseLockedMethod();
        assertThat(lockedService.counter.get()).isEqualTo(1);
    }

    @Test
    void shouldInjectFencingToken() {
        lockedService.lastToken.set(null);
        lockedService.leaseLockedWithToken();
        assertThat(lockedService.lastToken.get()).isNotNull();
        assertThat(lockedService.lastToken.get().value()).isPositive();
    }

    @Test
    void shouldExecuteWithAdvisoryLock() {
        lockedService.counter.set(0);
        lockedService.advisoryLockedMethod();
        assertThat(lockedService.counter.get()).isEqualTo(1);
    }

    @Test
    void shouldExecuteWithSessionLock() {
        lockedService.counter.set(0);
        lockedService.sessionLockedMethod();
        assertThat(lockedService.counter.get()).isEqualTo(1);
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestConfig {
        @Bean
        DataSource dataSource() {
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setUrl(PG.getJdbcUrl());
            ds.setUser(PG.getUsername());
            ds.setPassword(PG.getPassword());
            return ds;
        }

        @Bean
        LockedService lockedService() {
            return new LockedService();
        }
    }

    static class LockedService {
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicReference<FencingToken> lastToken = new AtomicReference<>();

        @FencepostLock(name = "integration-lease", lockAtMostFor = "1m")
        void leaseLockedMethod() {
            counter.incrementAndGet();
        }

        @FencepostLock(name = "integration-lease-token", lockAtMostFor = "1m")
        void leaseLockedWithToken(FencingToken token) {
            lastToken.set(token);
        }

        @FencepostLock(name = "integration-advisory", lockAtMostFor = "1m", type = LockType.ADVISORY)
        void advisoryLockedMethod() {
            counter.incrementAndGet();
        }

        @FencepostLock(name = "integration-session", lockAtMostFor = "1m", type = LockType.SESSION)
        void sessionLockedMethod() {
            counter.incrementAndGet();
        }
    }
}
```

- [ ] **Step 2: Run integration test**

```bash
mvn test -pl fencepost-spring -Dtest=FencepostSpringIntegrationTest
```

Expected: 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add fencepost-spring/src/test/
git commit -m "test(spring): add integration tests for @FencepostLock

End-to-end tests with Spring Boot context and Testcontainers PostgreSQL.
Covers LEASE, ADVISORY, SESSION lock types and fencing token injection."
```

---

### Task 10: Final Build Verification

**Files:** None (verification only).

- [ ] **Step 1: Run the full build from the root**

```bash
cd /Users/pivovarit/Dev/pivovarit/fencepost
mvn clean verify
```

Expected: BUILD SUCCESS for both modules. All core tests still pass. All spring tests pass.

- [ ] **Step 2: Verify no core module changes**

```bash
git diff fencepost-core/
```

Expected: No changes to core module source code (only the relocation from Task 1).

- [ ] **Step 3: Commit any final fixes if needed**

Only if the build revealed issues. Otherwise, skip.
