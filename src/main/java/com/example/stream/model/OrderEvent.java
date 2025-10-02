package com.example.stream.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

@JsonPropertyOrder({"order-id", "type", "timestamp", "order_details"})
public final class OrderEvent {
    private final String orderId;
    private final String type;
    private final Instant timestamp;
    private final JsonNode orderDetails;

    @JsonCreator
    public OrderEvent(
        @JsonProperty(value = "order-id", required = true) String orderId,
        @JsonProperty(value = "type", required = true) String type,
        @JsonProperty(value = "timestamp", required = true) Instant timestamp,
        @JsonProperty(value = "order_details", required = true) JsonNode orderDetails
    ) {
        this.orderId = orderId;
        this.type = type;
        this.timestamp = timestamp;
        this.orderDetails = orderDetails;
    }

    @JsonProperty("order-id")
    public String orderId() {
        return orderId;
    }

    @JsonProperty("type")
    public String type() {
        return type;
    }

    @JsonProperty("timestamp")
    public Instant timestamp() {
        return timestamp;
    }

    @JsonProperty("order_details")
    public JsonNode orderDetails() {
        return orderDetails;
    }

    public OrderEvent withType(String newType) {
        return new OrderEvent(orderId, newType, timestamp, orderDetails);
    }

    public Optional<OrderEventType> eventType() {
        return Optional.ofNullable(type)
            .flatMap(OrderEventType::fromRaw);
    }

    public String canonicalType() {
        return eventType()
            .map(OrderEventType::canonicalName)
            .orElseGet(() -> type == null ? null : type.toLowerCase(Locale.ROOT));
    }
}
