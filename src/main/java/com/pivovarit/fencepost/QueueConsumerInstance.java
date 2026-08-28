package com.pivovarit.fencepost;

import com.pivovarit.fencepost.function.ThrowingConsumer;
import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import com.pivovarit.fencepost.queue.LostOwnershipException;
import com.pivovarit.fencepost.queue.QueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

final class QueueConsumerInstance implements QueueConsumer {

    private static final Logger logger = LoggerFactory.getLogger(QueueConsumerInstance.class);
    private static final long CLOSE_TIMEOUT_MS = 10_000;
    private static final long ERROR_BACKOFF_MS = 1_000;

    private final String queueName;
    private final Queue queue;
    private final ThrowingConsumer<Message> handler;
    private final int concurrency;
    private final BiConsumer<Message, Throwable> onError;
    private final int maxDeliveries;
    private final Duration retryDelay;

    private final ExecutorService executor;
    private final Object lifecycleLock = new Object();
    private volatile boolean closed;
    private boolean started;

    QueueConsumerInstance(String queueName, Queue queue, ThrowingConsumer<Message> handler,
                          int concurrency, BiConsumer<Message, Throwable> onError,
                          int maxDeliveries, Duration retryDelay) {
        this.queueName = queueName;
        this.queue = queue;
        this.handler = handler;
        this.concurrency = concurrency;
        this.onError = onError;
        this.maxDeliveries = maxDeliveries;
        this.retryDelay = retryDelay;
        this.executor = Executors.newFixedThreadPool(concurrency, new ConsumerThreadFactory(queueName));
    }

    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("QueueConsumer has been closed: " + queueName);
            }
            if (started) {
                return;
            }
            started = true;
            for (int i = 0; i < concurrency; i++) {
                executor.submit(this::consumeLoop);
            }
            logger.debug("started {} consumer thread(s) for queue '{}'", concurrency, queueName);
        }
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        queue.close();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                logger.warn("consumer threads for queue '{}' did not exit within {} ms; forcing shutdown", queueName, CLOSE_TIMEOUT_MS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void consumeLoop() {
        while (!closed) {
            if (Thread.currentThread().isInterrupted()) {
                logger.warn("consumer thread for queue '{}' was interrupted; stopping", queueName);
                return;
            }
            try {
                dispatch(queue.dequeue());
            } catch (FencepostException e) {
                if (closed || Thread.currentThread().isInterrupted()) {
                    continue; // loop head exits without reporting or backing off
                }
                logger.debug("dequeue error on queue '{}': {}", queueName, e.getMessage());
                reportError(null, e);
                backoff();
            } catch (RuntimeException e) {
                if (closed || Thread.currentThread().isInterrupted()) {
                    continue; // loop head exits without reporting or backing off
                }
                logger.warn("unexpected error in consumer loop for queue '{}'", queueName, e);
                reportError(null, e);
                backoff();
            }
        }
    }

    private void backoff() {
        try {
            Thread.sleep(ERROR_BACKOFF_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void dispatch(Message msg) {
        try {
            handler.accept(msg);
        } catch (Throwable t) {
            failMessage(msg, t);
            return;
        }
        try {
            ackIfActive(msg);
        } catch (Throwable t) {
            reportError(msg, t);
        }
    }

    private void ackIfActive(Message msg) {
        if (!(msg instanceof AckableMessage am)) {
            msg.ack();
            return;
        }
        AckableMessage.State state = am.currentState();
        switch (state) {
            case ACTIVE -> am.ack();
            case CLOSED -> logger.warn("handler closed message {} on queue '{}' without acking or nacking; it will be redelivered after the visibility timeout",
              am.id(), queueName);
            default -> logger.debug("handler already {} message {} on queue '{}'; skipping auto-ack",
              state.name().toLowerCase(), am.id(), queueName);
        }
    }

    private void failMessage(Message msg, Throwable t) {
        try {
            if (msg instanceof AckableMessage am) {
                resolveFailed(am, t);
            } else {
                logger.warn("message {} on queue '{}' is not consumer-managed ({}); nacking without retryDelay/maxDeliveries",
                  msg.id(), queueName, msg.getClass().getName());
                msg.nack();
            }
        } catch (LostOwnershipException lost) {
            logger.debug("lost ownership resolving failed message {} on queue '{}'", msg.id(), queueName, lost);
        } catch (Exception resolveEx) {
            logger.warn("failed to resolve failed message {} on queue '{}'", msg.id(), queueName, resolveEx);
        }
        reportError(msg, t);
    }

    private void resolveFailed(AckableMessage msg, Throwable t) {
        switch (msg.currentState()) {
            case ACTIVE -> {
                if (maxDeliveries > 0 && msg.attempts() >= maxDeliveries) {
                    String reason = describe(t);
                    msg.deadLetter(reason);
                    logger.warn("dead-lettered message {} on queue '{}' after {} deliveries: {}",
                      msg.id(), queueName, msg.attempts(), reason);
                } else {
                    msg.nack(retryDelay);
                }
            }
            case ACKED -> logger.debug("handler acked message {} on queue '{}' before throwing; nothing to resolve",
              msg.id(), queueName);
            default -> logger.warn("message {} on queue '{}' was already {} when its handler threw; retryDelay/maxDeliveries not applied",
              msg.id(), queueName, msg.currentState().name().toLowerCase());
        }
    }

    private static final int MAX_ERROR_LENGTH = 1000;

    static String describe(Throwable t) {
        String s = t.toString();
        if (s == null) {
            s = t.getClass().getName();
        }
        s = s.replace('\0', ' ');
        if (s.length() <= MAX_ERROR_LENGTH) {
            return s;
        }
        int end = Character.isHighSurrogate(s.charAt(MAX_ERROR_LENGTH - 1)) ? MAX_ERROR_LENGTH - 1 : MAX_ERROR_LENGTH;
        return s.substring(0, end);
    }

    private void reportError(Message msg, Throwable t) {
        try {
            onError.accept(msg, t);
        } catch (Throwable inner) {
            logger.warn("onError handler itself threw for queue '{}'", queueName, inner);
        }
    }

    private static final class ConsumerThreadFactory implements ThreadFactory {
        private final String queueName;
        private final AtomicInteger counter = new AtomicInteger();

        ConsumerThreadFactory(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "fencepost-consumer-" + queueName + "-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
