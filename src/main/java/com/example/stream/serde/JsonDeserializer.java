package com.example.stream.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public final class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper;
    private final Class<T> targetType;

    public JsonDeserializer(ObjectMapper mapper, Class<T> targetType) {
        this.mapper = mapper;
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize payload from topic " + topic, e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
