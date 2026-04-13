package com.pivovarit.fencepost.spring;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
