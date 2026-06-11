package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Queue;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

final class TestQueues {

    private TestQueues() {
    }

    static void enqueue(DataSource dataSource, String queueName, String... messages) {
        Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName(queueName);
        for (String msg : messages) {
            queue.enqueue(msg.getBytes(UTF_8), "test", Map.of());
        }
    }
}
