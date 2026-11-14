package com.pivovarit.fencepost.queue;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * A dead-lettered message as stored in the queue table.
 *
 * @param id        the message id
 * @param payload   the message body
 * @param type      the caller-supplied type tag, if any
 * @param headers   the message headers, empty if none
 * @param attempts  how many times the message was delivered before it was dead-lettered
 * @param lastError description of the failure that dead-lettered the message, if recorded
 * @param deadAt    when the message was dead-lettered
 */
public record DeadMessage(
  long id,
  byte[] payload,
  Optional<String> type,
  Map<String, String> headers,
  int attempts,
  Optional<String> lastError,
  Instant deadAt) {
}
