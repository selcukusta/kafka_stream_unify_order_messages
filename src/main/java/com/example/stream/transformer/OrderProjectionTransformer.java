package com.example.stream.transformer;

import com.example.stream.OrderProjectionTopology;
import com.example.stream.model.OrderEvent;
import com.example.stream.model.OrderProjection;
import com.example.stream.model.OrderProjectionPayload;
import com.example.stream.model.PendingOrder;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OrderProjectionTransformer implements Transformer<String, OrderEvent, KeyValue<String, OrderProjectionPayload>> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderProjectionTransformer.class);
    private final Duration bufferDuration;
    private ProcessorContext context;
    private KeyValueStore<String, PendingOrder> store;

    public OrderProjectionTransformer(Duration bufferDuration) {
        this.bufferDuration = bufferDuration;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, PendingOrder>) context.getStateStore(OrderProjectionTopology.PENDING_ORDER_STORE);
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, this::flushExpired);
    }

    @Override
    public KeyValue<String, OrderProjectionPayload> transform(String key, OrderEvent value) {
        if (key == null || value == null) {
            return null;
        }
        long now = context.currentSystemTimeMs();

        PendingOrder pending = store.get(key);
        if (pending != null) {
            System.out.printf("[transform] current buffer %s deadline %d types=%s%n", key, pending.deadlineEpochMillis(), pending.projection() != null ? pending.projection().events().keySet() : null);
        }
        if (pending != null) {
            OrderProjection existingProjection = pending.projection();
            long deadline = pending.deadlineEpochMillis();
            if (existingProjection == null || deadline <= 0 || now >= deadline) {
                LOG.debug("Existing buffer for order {} is expired or invalid (deadline {}, now {}), flushing before update", key, deadline, now);
                System.out.printf("[transform] expiring stale buffer for %s deadline %d now %d%n", key, deadline, now);
                emitAndRemove(key, pending);
                pending = null;
            }
        }

        OrderProjection current = pending != null ? pending.projection() : null;
        if (pending == null || current == null) {
            long deadline = now + bufferDuration.toMillis();
            PendingOrder next = new PendingOrder(OrderProjection.empty().addEvent(value), deadline);
            store.put(key, next);
            PendingOrder stored = store.get(key);
            System.out.printf("[transform] after put buffer %s deadline %d types=%s%n", key, stored != null ? stored.deadlineEpochMillis() : -1, stored != null && stored.projection() != null ? stored.projection().events().keySet() : null);
            LOG.debug("Started buffer for order {} with deadline {}", key, deadline);
            System.out.printf("[transform] started buffer for %s with deadline %d%n", key, deadline);
        } else {
            OrderProjection updated = current.addEvent(value);
            if (updated != current) {
                store.put(key, pending.withProjection(updated));
                PendingOrder stored = store.get(key);
                System.out.printf("[transform] after merge buffer %s deadline %d types=%s%n", key, stored != null ? stored.deadlineEpochMillis() : -1, stored != null && stored.projection() != null ? stored.projection().events().keySet() : null);
                LOG.debug("Buffered additional event for order {}. Types now {}", key, updated.events().keySet());
                System.out.printf("[transform] buffered event for %s types=%s%n", key, updated.events().keySet());
            } else {
                LOG.debug("Ignoring duplicate event type for order {}", key);
                System.out.printf("[transform] duplicate event type for %s ignored%n", key);
            }
        }
        return null;
    }

    @Override
    public void close() {
        // no-op
    }

    private void flushExpired(long timestamp) {
        try (KeyValueIterator<String, PendingOrder> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, PendingOrder> entry = iterator.next();
                long deadline = entry.value.deadlineEpochMillis();
                if (timestamp >= deadline) {
                    long waited = timestamp - deadline;
                    LOG.debug("Flushing order {} at timestamp {} (waited {} ms past deadline)", entry.key, timestamp, waited);
                    System.out.printf("[punctuate] flushing order %s at %d (waited %+d ms) deadline %d%n", entry.key, timestamp, waited, deadline);
                    emitAndRemove(entry.key, entry.value);
                }
            }
        }
        context.commit();
    }

    private void emitAndRemove(String key, PendingOrder pending) {
        if (pending == null) {
            return;
        }
        OrderProjection projection = pending.projection();
        if (projection == null || !projection.isComplete()) {
            LOG.debug("Projection for order {} incomplete; dropping", key);
            System.out.printf("[emit] incomplete projection for %s dropped%n", key);
            store.delete(key);
            return;
        }
        OrderProjectionPayload payload = projection.toPayload();
        if (payload != null) {
            LOG.info("Emitting projection for order {} with {} events", key, payload.events().size());
            System.out.printf("[emit] projection for %s with %d events emitted%n", key, payload.events().size());
            context.forward(key, payload);
        }
        store.delete(key);
    }
}
