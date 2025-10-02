package com.example.stream.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public final class OrderProjectionPayload {
    private final String orderId;
    private final List<OrderEvent> events;

    @JsonCreator
    public OrderProjectionPayload(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("events") List<OrderEvent> events
    ) {
        this.orderId = orderId;
        this.events = events == null ? List.of() : List.copyOf(events);
    }

    @JsonProperty("orderId")
    public String orderId() {
        return orderId;
    }

    @JsonProperty("events")
    public List<OrderEvent> events() {
        return events;
    }
}
