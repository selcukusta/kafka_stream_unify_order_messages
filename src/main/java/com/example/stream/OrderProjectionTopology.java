package com.example.stream;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;

import com.example.stream.model.OrderEvent;
import com.example.stream.model.OrderEventType;
import com.example.stream.model.OrderProjectionPayload;
import com.example.stream.model.PendingOrder;
import com.example.stream.serde.JsonSerde;
import com.example.stream.transformer.OrderProjectionTransformer;

public final class OrderProjectionTopology {
        public static final String ORDER_CREATED_TOPIC = "order-created";
        public static final String ORDER_PLACED_TOPIC = "order-placed";
        public static final String ORDER_CANCELLED_TOPIC = "order-cancelled";
        public static final String ORDER_PROJECTION_TOPIC = "order-projection";

        public static final String PENDING_ORDER_STORE = "pending-order-store";
        private static final Duration BUFFER_DURATION = Duration.ofSeconds(5);

        public Topology build() {
                StreamsBuilder builder = new StreamsBuilder();

                Serde<String> stringSerde = Serdes.String();
                Serde<OrderEvent> orderEventSerde = JsonSerde.forType(OrderEvent.class);
                Serde<OrderProjectionPayload> payloadSerde = JsonSerde.forType(OrderProjectionPayload.class);
                Serde<PendingOrder> pendingOrderSerde = JsonSerde.forType(PendingOrder.class);

                KStream<String, OrderEvent> createdStream = topicStream(builder, ORDER_CREATED_TOPIC,
                                OrderEventType.CREATED,
                                orderEventSerde);
                KStream<String, OrderEvent> placedStream = topicStream(builder, ORDER_PLACED_TOPIC,
                                OrderEventType.PLACED,
                                orderEventSerde);
                KStream<String, OrderEvent> cancelledStream = topicStream(builder, ORDER_CANCELLED_TOPIC,
                                OrderEventType.CANCELLED, orderEventSerde);

                builder.addStateStore(
                                Stores.keyValueStoreBuilder(
                                                Stores.persistentKeyValueStore(PENDING_ORDER_STORE),
                                                stringSerde,
                                                pendingOrderSerde).withCachingDisabled());

                KStream<String, OrderEvent> allEvents = createdStream
                                .merge(placedStream)
                                .merge(cancelledStream)
                                .repartition(Repartitioned.<String, OrderEvent>as("order-events-by-id")
                                                .withKeySerde(stringSerde)
                                                .withValueSerde(orderEventSerde));

                allEvents
                                .transform(() -> new OrderProjectionTransformer(BUFFER_DURATION), PENDING_ORDER_STORE)
                                .filter((orderId, payload) -> payload != null)
                                .to(ORDER_PROJECTION_TOPIC, Produced.with(stringSerde, payloadSerde));

                return builder.build();
        }

        private KStream<String, OrderEvent> topicStream(
                        StreamsBuilder builder,
                        String topic,
                        OrderEventType fallbackType,
                        Serde<OrderEvent> orderEventSerde) {
                return builder.stream(topic, Consumed.with(Serdes.String(), orderEventSerde))
                                .peek((key, value) -> System.out.printf("[source:%s] key=%s rawType=%s timestamp=%s%n",
                                                topic, key,
                                                value != null ? value.type() : null,
                                                value != null ? value.timestamp() : null))
                                .mapValues(event -> canonicalizeType(event, fallbackType))
                                .filter((key, value) -> value != null && value.orderId() != null)
                                .selectKey((key, value) -> value.orderId());
        }

        private OrderEvent canonicalizeType(OrderEvent event, OrderEventType fallbackType) {
                if (event == null) {
                        return null;
                }
                OrderEventType type = event.eventType().orElse(fallbackType);
                if (type == null) {
                        return event;
                }
                String canonicalType = type.canonicalName();
                if (canonicalType.equals(event.type())) {
                        return event;
                }
                return event.withType(canonicalType);
        }
}
