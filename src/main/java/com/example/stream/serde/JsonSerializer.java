package com.example.stream.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public final class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper;

    public JsonSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize payload for topic " + topic, e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
