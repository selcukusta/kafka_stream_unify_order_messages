package com.example.stream.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class PendingOrder {
    private final OrderProjection projection;
    private final long deadlineEpochMillis;

    @JsonCreator
    public PendingOrder(
        @JsonProperty("projection") OrderProjection projection,
        @JsonProperty("deadlineEpochMillis") long deadlineEpochMillis
    ) {
        this.projection = projection;
        this.deadlineEpochMillis = deadlineEpochMillis;
    }

    @JsonProperty("projection")
    public OrderProjection projection() {
        return projection;
    }

    @JsonProperty("deadlineEpochMillis")
    public long deadlineEpochMillis() {
        return deadlineEpochMillis;
    }

    public PendingOrder withProjection(OrderProjection updatedProjection) {
        if (updatedProjection == projection) {
            return this;
        }
        return new PendingOrder(updatedProjection, deadlineEpochMillis);
    }
}
