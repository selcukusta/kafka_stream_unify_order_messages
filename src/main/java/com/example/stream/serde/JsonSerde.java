package com.example.stream.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class JsonSerde<T> extends Serdes.WrapperSerde<T> {
    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    static {
        DEFAULT_MAPPER.registerModule(new JavaTimeModule());
        DEFAULT_MAPPER.findAndRegisterModules();
        DEFAULT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public JsonSerde(Class<T> targetType) {
        super(new JsonSerializer<>(DEFAULT_MAPPER), new JsonDeserializer<>(DEFAULT_MAPPER, targetType));
    }

    public static <R> Serde<R> forType(Class<R> type) {
        return new JsonSerde<>(type);
    }
}
