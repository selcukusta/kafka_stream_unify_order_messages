package com.example.stream.model;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public enum OrderEventType {
    CREATED(10, "created"),
    PLACED(20, "placed"),
    CANCELLED(30, "cancelled");

    private static final Map<String, OrderEventType> BY_NAME = Map.ofEntries(
        Map.entry(CREATED.canonicalName, CREATED),
        Map.entry("order-created", CREATED),
        Map.entry(PLACED.canonicalName, PLACED),
        Map.entry("order-placed", PLACED),
        Map.entry(CANCELLED.canonicalName, CANCELLED),
        Map.entry("order-cancelled", CANCELLED)
    );

    private final int priority;
    private final String canonicalName;

    OrderEventType(int priority, String canonicalName) {
        this.priority = priority;
        this.canonicalName = canonicalName;
    }

    public int priority() {
        return priority;
    }

    public String canonicalName() {
        return canonicalName;
    }

    public static Optional<OrderEventType> fromRaw(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        String normalized = raw.toLowerCase(Locale.ROOT);
        return Optional.ofNullable(BY_NAME.get(normalized));
    }
}
