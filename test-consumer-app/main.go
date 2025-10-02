package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type OrderEvent struct {
	OrderID      string                 `json:"order-id"`
	Type         string                 `json:"type"`
	Timestamp    string                 `json:"timestamp"`
	OrderDetails map[string]interface{} `json:"order_details"`
}

type Config struct {
	Iterations          int
	TestMissingEvents   bool
	TestDuplicateEvents bool
	Probability         float64
}

func main() {
	iterations := flag.Int("iterations", 1, "Number of iterations")
	testMissing := flag.Bool("test_missing_events", false, "Enable missing events test")
	testDuplicate := flag.Bool("test_duplicate_events", false, "Enable duplicate events test")
	probability := flag.Float64("probability", 0.0, "Probability for missing/duplicate events (0.0-1.0)")
	flag.Parse()

	if *testMissing && *testDuplicate {
		log.Fatal("Error: test_missing_events and test_duplicate_events cannot be used at the same time")
	}

	if *probability < 0.0 || *probability > 1.0 {
		log.Fatal("Error: probability must be between 0.0 and 1.0")
	}

	config := Config{
		Iterations:          *iterations,
		TestMissingEvents:   *testMissing,
		TestDuplicateEvents: *testDuplicate,
		Probability:         *probability,
	}

	mechanism, err := scram.Mechanism(scram.SHA256, "superuser", "secretpassword")
	if err != nil {
		log.Fatalf("Failed to create SCRAM mechanism: %v", err)
	}

	brokers := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
	topics := []string{"order-created", "order-placed", "order-cancelled", "order-projection"}

	// Ensure topics exist before publishing
	if err := ensureTopicsExist(brokers, topics, mechanism); err != nil {
		log.Fatalf("Failed to ensure topics exist: %v", err)
	}

	writers := map[string]*kafka.Writer{
		"order-created": kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{"localhost:19092", "localhost:29092", "localhost:39092"},
			Topic:   "order-created",
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
			},
		}),
		"order-placed": kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{"localhost:19092", "localhost:29092", "localhost:39092"},
			Topic:   "order-placed",
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
			},
		}),
		"order-cancelled": kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{"localhost:19092", "localhost:29092", "localhost:39092"},
			Topic:   "order-cancelled",
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
			},
		}),
	}

	defer func() {
		for _, w := range writers {
			w.Close()
		}
	}()

	rand.Seed(time.Now().UnixNano())

	log.Printf("Starting to publish events: iterations=%d, test_missing=%v, test_duplicate=%v, probability=%.2f\n",
		config.Iterations, config.TestMissingEvents, config.TestDuplicateEvents, config.Probability)

	// Pre-calculate which iterations should have missing/duplicate events
	affectedIterations := make(map[int]bool)
	if config.TestMissingEvents || config.TestDuplicateEvents {
		expectedAffected := int(float64(config.Iterations) * config.Probability)
		if expectedAffected > 0 {
			// Randomly select iterations to be affected
			iterationList := rand.Perm(config.Iterations)
			for i := 0; i < expectedAffected; i++ {
				affectedIterations[iterationList[i]] = true
			}
			log.Printf("Will affect %d out of %d iterations (affected iterations: %v)\n",
				expectedAffected, config.Iterations, getAffectedList(affectedIterations, config.Iterations))
		}
	}

	for i := 0; i < config.Iterations; i++ {
		log.Printf("Iteration %d/%d\n", i+1, config.Iterations)

		shouldApplyTest := affectedIterations[i]
		if err := publishOrderEvents(writers, config, shouldApplyTest); err != nil {
			log.Printf("Error in iteration %d: %v\n", i+1, err)
		}

		if i < config.Iterations-1 {
			time.Sleep(1 * time.Second)
		}
	}

	log.Println("All events published successfully")
}

func ensureTopicsExist(brokers []string, topics []string, mechanism sasl.Mechanism) error {
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	conn, err := dialer.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := dialer.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	// Get existing topics
	partitions, err := controllerConn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	existingTopics := make(map[string]bool)
	for _, p := range partitions {
		existingTopics[p.Topic] = true
	}

	// Create topics that don't exist
	topicsToCreate := []kafka.TopicConfig{}
	for _, topic := range topics {
		if !existingTopics[topic] {
			log.Printf("Topic '%s' does not exist, creating...\n", topic)
			topicsToCreate = append(topicsToCreate, kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     3,
				ReplicationFactor: 1,
			})
		} else {
			log.Printf("Topic '%s' already exists\n", topic)
		}
	}

	if len(topicsToCreate) > 0 {
		err = controllerConn.CreateTopics(topicsToCreate...)
		if err != nil {
			return fmt.Errorf("failed to create topics: %w", err)
		}
		log.Printf("Successfully created %d topic(s)\n", len(topicsToCreate))
	}

	return nil
}

func getAffectedList(affectedMap map[int]bool, total int) []int {
	affected := []int{}
	for i := 0; i < total; i++ {
		if affectedMap[i] {
			affected = append(affected, i+1) // +1 for 1-indexed display
		}
	}
	return affected
}

func publishOrderEvents(writers map[string]*kafka.Writer, config Config, shouldApplyTest bool) error {
	orderID := uuid.New().String()
	baseTime := time.Now()

	// Generate timestamps with max 100ms gap
	timestamps := make([]time.Time, 3)
	timestamps[0] = baseTime
	timestamps[1] = timestamps[0].Add(time.Duration(rand.Intn(101)) * time.Millisecond)
	timestamps[2] = timestamps[1].Add(time.Duration(rand.Intn(101)) * time.Millisecond)

	// Create events
	events := []struct {
		topic string
		event OrderEvent
	}{
		{
			topic: "order-created",
			event: OrderEvent{
				OrderID:   orderID,
				Type:      "created",
				Timestamp: timestamps[0].Format("2006-01-02T15:04:05.999Z07:00"),
				OrderDetails: map[string]interface{}{
					"customer_id": fmt.Sprintf("C-%d", rand.Intn(9000)+1000),
					"items": []map[string]interface{}{
						{
							"sku":        fmt.Sprintf("BOOK-%d", rand.Intn(9000)+1000),
							"quantity":   rand.Intn(3) + 1,
							"unit_price": float64(rand.Intn(10000)) / 100,
						},
						{
							"sku":        fmt.Sprintf("PEN-%d", rand.Intn(9000)+1000),
							"quantity":   rand.Intn(5) + 1,
							"unit_price": float64(rand.Intn(2000)) / 100,
						},
					},
					"total":    float64(rand.Intn(50000)) / 100,
					"currency": "USD",
				},
			},
		},
		{
			topic: "order-placed",
			event: OrderEvent{
				OrderID:   orderID,
				Type:      "placed",
				Timestamp: timestamps[1].Format("2006-01-02T15:04:05.999Z07:00"),
				OrderDetails: map[string]interface{}{
					"payment_reference": fmt.Sprintf("PAY-%d", rand.Intn(900000)+100000),
					"shipping_method":   "standard",
					"shipping_address": map[string]string{
						"line1":       "742 Evergreen Terrace",
						"city":        "Springfield",
						"state":       "IL",
						"postal_code": "62704",
						"country":     "US",
					},
				},
			},
		},
		{
			topic: "order-cancelled",
			event: OrderEvent{
				OrderID:   orderID,
				Type:      "cancelled",
				Timestamp: timestamps[2].Format("2006-01-02T15:04:05.999Z07:00"),
				OrderDetails: map[string]interface{}{
					"reason":           "customer_changed_mind",
					"cancelled_by":     "customer_support",
					"refund_reference": fmt.Sprintf("RF-%d", rand.Intn(900000)+100000),
				},
			},
		},
	}

	// Apply missing events logic
	if config.TestMissingEvents && shouldApplyTest {
		events = applyMissingEvents(events)
	}

	// Apply duplicate events logic
	eventsToPublish := events
	if config.TestDuplicateEvents && shouldApplyTest {
		eventsToPublish = applyDuplicateEvents(events)
	}

	// Shuffle events to simulate out-of-order delivery
	rand.Shuffle(len(eventsToPublish), func(i, j int) {
		eventsToPublish[i], eventsToPublish[j] = eventsToPublish[j], eventsToPublish[i]
	})

	// Publish events
	ctx := context.Background()
	for _, e := range eventsToPublish {
		msgBytes, err := json.Marshal(e.event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		err = writers[e.topic].WriteMessages(ctx, kafka.Message{
			Key:   []byte(e.event.OrderID),
			Value: msgBytes,
		})
		if err != nil {
			return fmt.Errorf("failed to write message to %s: %w", e.topic, err)
		}

		log.Printf("Published to %s: order-id=%s, type=%s, timestamp=%s\n",
			e.topic, e.event.OrderID, e.event.Type, e.event.Timestamp)
	}

	return nil
}

func applyMissingEvents(events []struct {
	topic string
	event OrderEvent
}) []struct {
	topic string
	event OrderEvent
} {
	// Randomly decide which events to skip
	skipPattern := rand.Intn(3)

	switch skipPattern {
	case 0:
		// Skip created (send only placed and cancelled)
		log.Println("  [MISSING EVENT] Skipping 'created' event")
		return events[1:]
	case 1:
		// Skip placed (send only created and cancelled)
		log.Println("  [MISSING EVENT] Skipping 'placed' event")
		return append(events[:1], events[2:]...)
	case 2:
		// Send only created
		log.Println("  [MISSING EVENT] Skipping 'placed' and 'cancelled' events")
		return events[:1]
	}

	return events
}

func applyDuplicateEvents(events []struct {
	topic string
	event OrderEvent
}) []struct {
	topic string
	event OrderEvent
} {
	result := make([]struct {
		topic string
		event OrderEvent
	}, 0)

	// Randomly select which event(s) to duplicate
	eventsToDuplicate := rand.Intn(3)

	for i, e := range events {
		result = append(result, e)

		// Check if this event should be duplicated
		if i == eventsToDuplicate || (eventsToDuplicate == 2 && i < 2) {
			duplicateCount := rand.Intn(4) + 2 // 2-5 duplicates
			log.Printf("  [DUPLICATE EVENT] Duplicating '%s' event %d times\n", e.event.Type, duplicateCount)

			for j := 0; j < duplicateCount-1; j++ {
				result = append(result, e)
			}
		}
	}

	return result
}
