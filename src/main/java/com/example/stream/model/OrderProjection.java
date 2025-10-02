package com.example.stream.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OrderProjection {
    private final String orderId;
    private final Map<OrderEventType, OrderEvent> events;
    private static final Set<OrderEventType> REQUIRED_TYPES =
        Collections.unmodifiableSet(EnumSet.allOf(OrderEventType.class));

    public static OrderProjection empty() {
        return new OrderProjection(null, new EnumMap<>(OrderEventType.class));
    }

    @JsonCreator
    public OrderProjection(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("events") Map<OrderEventType, OrderEvent> events
    ) {
        this.orderId = orderId;
        this.events = events == null
            ? new EnumMap<>(OrderEventType.class)
            : new EnumMap<>(events);
    }

    @JsonProperty("orderId")
    public String orderId() {
        return orderId;
    }

    @JsonProperty("events")
    public Map<OrderEventType, OrderEvent> events() {
        return Collections.unmodifiableMap(events);
    }

    public OrderProjection addEvent(OrderEvent event) {
        if (event == null || event.orderId() == null) {
            return this;
        }
        OrderEventType type = event.eventType().orElse(null);
        if (type == null) {
            return this;
        }
        EnumMap<OrderEventType, OrderEvent> updated = new EnumMap<>(events);
        if (updated.containsKey(type)) {
            return this;
        }
        updated.put(type, event);
        String id = orderId != null ? orderId : event.orderId();
        return new OrderProjection(id, updated);
    }

    public OrderProjection merge(OrderProjection other) {
        if (other == null) {
            return this;
        }
        EnumMap<OrderEventType, OrderEvent> merged = new EnumMap<>(events);
        other.events.forEach((type, event) -> merged.putIfAbsent(type, event));
        String id = orderId != null ? orderId : other.orderId;
        return new OrderProjection(id, merged);
    }

    public List<OrderEvent> orderedEvents() {
        List<OrderEvent> ordered = new ArrayList<>(events.values());
        ordered.sort((left, right) -> {
            OrderEventType leftType = left.eventType()
                .orElseThrow(() -> new IllegalStateException("Unknown event type for order " + left.orderId()));
            OrderEventType rightType = right.eventType()
                .orElseThrow(() -> new IllegalStateException("Unknown event type for order " + right.orderId()));
            return Integer.compare(leftType.priority(), rightType.priority());
        });
        return ordered;
    }

    public OrderProjectionPayload toPayload() {
        if (orderId == null || !isComplete()) {
            return null;
        }
        List<OrderEvent> ordered = orderedEvents();
        return new OrderProjectionPayload(orderId, ordered);
    }

    public boolean isComplete() {
        return events.keySet().containsAll(REQUIRED_TYPES);
    }
}
