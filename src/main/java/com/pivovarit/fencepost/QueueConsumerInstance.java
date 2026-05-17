package com.pivovarit.fencepost;

import com.pivovarit.fencepost.function.ThrowingConsumer;
import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import com.pivovarit.fencepost.queue.QueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final ExecutorService executor;
    private final Object lifecycleLock = new Object();
    private volatile boolean closed;
    private boolean started;

    QueueConsumerInstance(String queueName, Queue queue, ThrowingConsumer<Message> handler,
                          int concurrency, BiConsumer<Message, Throwable> onError) {
        this.queueName = queueName;
        this.queue = queue;
        this.handler = handler;
        this.concurrency = concurrency;
        this.onError = onError;
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
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                logger.warn("consumer threads for queue '{}' did not exit within {} ms", queueName, CLOSE_TIMEOUT_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void consumeLoop() {
        while (!closed) {
            try {
                dispatch(queue.dequeue());
            } catch (FencepostException e) {
                if (closed) {
                    return;
                }
                logger.debug("dequeue error on queue '{}': {}", queueName, e.getMessage());
                reportError(null, e);
                backoff();
            } catch (RuntimeException e) {
                if (closed) {
                    return;
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
            reportError(msg, t);
            try {
                msg.nack();
            } catch (Exception nackEx) {
                logger.trace("nack failed for message {} on queue '{}'", msg.id(), queueName, nackEx);
            }
            return;
        }
        try {
            msg.ack();
        } catch (Throwable t) {
            reportError(msg, t);
        }
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
