package com.pivovarit.fencepost;

final class QueueChannels {

    private QueueChannels() {
    }

    static String name(String queueName) {
        return "fencepost_q_" + Long.toUnsignedString(HashUtils.fnv1a64("fencepost:" + queueName));
    }
}
