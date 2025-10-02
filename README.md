# Order Projection Kafka Streams App

This project ingests lifecycle events from three topics (`order-created`, `order-placed`, `order-cancelled`), buffers them for up to 60 seconds per `order-id`, and emits a single ordered projection to `order-projection` when all required events arrive. Duplicates are ignored and incomplete lifecycles are dropped.

## Quick-start Workflow

Run the following steps in three separate terminal tabs.

### 1. Start Redpanda (first tab)

```bash
cd docker-compose
docker compose up -d
```

If the command fails (for example during initial network setup), run it again—the services usually come up cleanly on the second attempt.

### 2. Launch the Kafka Streams app (second tab)

From the repository root:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)    # install with `brew install temurin17` if you do not have JDK 17
rm -rf /tmp/kafka-streams                           # clear previous local state
export JAVA_TOOL_OPTIONS="-Djava.security.manager=allow"
mvn clean package
mvn exec:java -Dexec.args='streams.properties' -Dorg.slf4j.simpleLogger.defaultLogLevel=info
```

Leave this tab running. The final command starts the Streams topology and prints INFO-level logs that describe buffered events and emitted projections.

### 3. Produce sample traffic (third tab)

```bash
cd test-consumer-app
go get .
go run main.go --iterations=0   # bootstrap the Kafka topics
```

Confirm the four topics exist at <http://localhost:8080/topics>. Once `order-created`, `order-placed`, `order-cancelled`, and `order-projection` are present, publish synthetic lifecycles:

```bash
go run main.go --iterations=10
```

The Go producer generates complete, missing, and duplicate event sequences so you can observe how the topology handles each case. Review `test-consumer-app/main.go` to tweak iteration counts or message patterns.

## Maven Commands (standalone)

To build or run without the full workflow:

```bash
mvn clean package
mvn exec:java -Dexec.args='streams.properties'
```

## Gradle Alternative

If you prefer Gradle and already have the wrapper, ensure you run it with JDK 17 or 21:

```bash
gradle wrapper --gradle-version 8.7   # only once if ./gradlew is missing
./gradlew clean build
```

## Troubleshooting Tips

- **Missing Projections:** The topology repartitions events by `order-id`, so all lifecycle messages must be produced within 5 real seconds. If an order is missing, check the logs for `incomplete projection … dropped` lines—they indicate at least one lifecycle event never arrived before the buffer expired.
- **SASL Credentials:** The Redpanda compose file enables SCRAM (`superuser/secretpassword`). Ensure every CLI producer/consumer and the Streams app use the same credentials (see `streams.properties` and `test-consumer-app/main.go`).
- **State Reset:** When changing topology code, clear `/tmp/kafka-streams` and run `kafka-streams-application-reset --application-id order-projection-app …` to wipe internal topics before restarting.

With Redpanda running, the Streams app started, and the Go producer generating events, the `order-projection` topic will contain one JSON payload per complete order lifecycle.
