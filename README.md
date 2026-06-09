# FluxMQ

FluxMQ is a small Spring Boot compatible SDK that provides event-style messaging abstractions over
ZeroMQ through JeroMQ.

It is designed for lightweight, best-effort PUB/SUB messaging inside applications that already
understand ZeroMQ's tradeoffs.

## What FluxMQ Is Not

FluxMQ is not a broker. It does not provide durable queues, persistent delivery, replay, consumer
groups, exactly-once delivery, distributed acknowledgements, distributed transactions, clustering,
schema registry, stream processing, or platform event-bus semantics.

If you need those guarantees, use Kafka, RabbitMQ, NATS JetStream, Pulsar, SQS, Pub/Sub, or another
broker/runtime designed for them.

## Installation

```xml
<dependency>
  <groupId>io.github.riken127.fluxmq</groupId>
  <artifactId>fluxmq-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

FluxMQ targets Java 25 and Spring Boot 3.x.

## Quick Start

```yaml
fluxmq:
  enabled: true
  publisher:
    bind: tcp://*:5556
    queue-capacity: 1024
    enqueue-timeout: 5s
  subscriber:
    connect: tcp://localhost:5556
    topics:
      - orders.created
  shutdown:
    timeout: 5s
  serialization:
    format: json
  retry:
    max-attempts: 1
    delay: 0s
```

### Publishing

```java
@Service
public final class OrderService {
  private static final FluxMqTopic<OrderCreated> ORDERS_CREATED =
      FluxMqTopic.of("orders.created", OrderCreated.class);

  private final FluxMqPublisher publisher;

  public OrderService(FluxMqPublisher publisher) {
    this.publisher = publisher;
  }

  public void createOrder(UUID orderId) {
    publisher.publish(ORDERS_CREATED, new OrderCreated(orderId));
  }
}
```

### Listening

```java
@Component
public final class OrderEventHandler {

  @FluxMqListener("orders.created")
  public void onOrderCreated(OrderCreated event) {
    // handle event
  }
}
```

Listeners may also receive headers:

```java
@FluxMqListener("orders.created")
public void onOrderCreated(OrderCreated event, FluxMqHeaders headers) {
  // handle event with metadata
}
```

## Configuration Reference

| Property | Default | Description |
| --- | --- | --- |
| `fluxmq.enabled` | `true` | Enables auto-configuration. |
| `fluxmq.publisher.bind` | none | Endpoint bound by the PUB socket. |
| `fluxmq.publisher.connect` | none | Endpoint connected by the PUB socket. |
| `fluxmq.publisher.queue-capacity` | `1024` | Bounded queue between application threads and the PUB socket worker. |
| `fluxmq.publisher.enqueue-timeout` | `5s` | Time a publish call waits for queue space. |
| `fluxmq.subscriber.bind` | none | Endpoint bound by the SUB socket. |
| `fluxmq.subscriber.connect` | none | Endpoint connected by the SUB socket. |
| `fluxmq.subscriber.topics` | empty | Extra SUB topics in addition to discovered listeners. |
| `fluxmq.shutdown.timeout` | `5s` | Time to wait for the subscriber loop to stop. |
| `fluxmq.serialization.format` | `json` | Built-in formats are `json`, `string`, and `bytes`. |
| `fluxmq.retry.max-attempts` | `1` | Local listener invocation attempts. `1` disables retry. |
| `fluxmq.retry.delay` | `0s` | Delay between local listener retry attempts. |

Publisher and subscriber roles support either `bind` or `connect`, not both. A configured publisher
requires a publisher endpoint. A subscriber endpoint is required when listeners or subscriber topics
are active.

## Serialization

The default `FluxMqSerializer` is selected from a small `FluxMqSerializerRegistry`. Built-in formats:

- `json`: Jackson object mapping.
- `string`: UTF-8 `String` payloads.
- `bytes`: raw `byte[]` payloads.

Applications may provide their own `FluxMqSerializer` bean directly, or provide a custom
`FluxMqSerializerRegistry` if they want named formats without replacing the public API.

## Observability Hooks

Implement `FluxMqObservationHandler` to receive lightweight publish and listener signals. FluxMQ does
not require or configure a metrics/tracing backend; the default handler is no-op.

## Delivery Guarantees And Limitations

FluxMQ uses ZeroMQ PUB/SUB. Delivery is best-effort. Messages can be dropped when subscribers are
disconnected, slow, not yet subscribed, or when the process exits before data is observed. FluxMQ
does not store messages or replay missed events.

Publisher calls enqueue work onto a bounded worker that owns the PUB socket, then wait for that send
operation to complete. This gives clearer socket thread ownership without adding async delivery
guarantees.

Listener retry is local and synchronous. It retries handler invocation in the subscriber process; it
does not request redelivery, persist messages, or acknowledge delivery to a publisher.

Design handlers to be idempotent when publishing meaningful business events.

## ZeroMQ PUB/SUB Caveats

- Startup ordering matters because subscribers need time to connect and subscribe.
- PUB/SUB sockets can drop early messages during connection setup.
- Sockets should not be casually shared across arbitrary application threads.
- FluxMQ keeps the MVP simple: one PUB socket, one SUB receive loop, synchronous handler invocation.

## Recommended Use Cases

- Production cache invalidation and refresh hints.
- Control-plane broadcasts where the latest state matters most.
- Live dashboards, status streams, and edge telemetry where gaps are acceptable.
- Lightweight service-to-service notifications inside trusted networks.

See [FluxMQ Use Cases](docs/use-cases.md) for concrete scenarios,
[Production Readiness](docs/production-readiness.md) for operational guidance, and
[`examples/cache-invalidation`](examples/cache-invalidation) for an end-to-end production-style
example.

## Non-Recommended Use Cases

- Work queues that must not lose messages.
- Financial, audit, or compliance flows requiring durable delivery.
- Event sourcing, replay, consumer groups, or broker-managed backpressure.
- Distributed workflows needing acknowledgements or transactions.

## Local Development

```bash
make format
make format-check
make lint
make test
make verify
make verify-examples
```

Fast tests are named `*Test`. In-process JeroMQ integration tests are named `*IntegrationTest` and
run during `make verify`. Example applications are checked by `make verify-examples`.

See `examples/basic-pubsub` for a minimal Spring Boot application and
`examples/cache-invalidation` for an end-to-end cache invalidation pattern.
