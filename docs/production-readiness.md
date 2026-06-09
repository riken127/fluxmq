# FluxMQ Production Readiness

FluxMQ can be used in production for advisory events: cache invalidations, refresh hints, live
status, local telemetry, control-plane broadcasts, and other signals where correctness comes from a
separate source of truth. It should not be used as the durable handoff for business-critical work.

## Current Support Level

| Area | Current support | Production guidance |
| --- | --- | --- |
| Best-effort PUB/SUB | Supported through one JeroMQ PUB socket and one SUB receive loop. | Use only when missed messages are acceptable. |
| Topic contracts | `FluxMqTopic<T>` can pair topic names with payload classes at compile time. | Treat this as a caller-side API contract, not a schema registry. |
| Serialization | Built-in `json`, `string`, and `bytes` serializers. | Keep payloads small and version tolerant. |
| Publisher backpressure | Publisher calls wait for a bounded worker queue and fail when enqueue timeout expires. | Size `queue-capacity` and `enqueue-timeout` for the expected burst profile. |
| Listener retry | Local synchronous retry on the subscriber thread. | Use for transient handler failures only; it is not transport redelivery. |
| Observability | `FluxMqObservationHandler` exposes publish and listener success/failure callbacks. | Bridge this to metrics, traces, or logs in the application. |
| Lifecycle | Spring manages the subscriber container through `SmartLifecycle`; publisher is closed as a bean. | Coordinate startup so subscribers are connected before important publishers send. |
| Reconnect and replay | No FluxMQ-level reconnect, persistence, replay, or catch-up. | Consumers must recover from the source of truth or wait for the next signal. |

## Operational Pattern

Production FluxMQ messages should normally be shaped as hints:

1. Write durable state to the real source of truth.
2. Publish a FluxMQ event containing the smallest useful identifier or version.
3. Subscribers refetch current state or invalidate local state.
4. Subscribers periodically reconcile or rely on TTL/version checks.

This pattern keeps FluxMQ out of the durability path while still reducing latency between a write and
its derived effects.

## Startup And Readiness

ZeroMQ PUB/SUB has a startup race: a publisher can send before subscribers are connected and
subscribed. FluxMQ does not add a coordination protocol on top of ZeroMQ.

Recommended production practices:

- start subscriber processes before publisher processes when early messages matter;
- make first publishes idempotent and repeatable where possible;
- use periodic state broadcasts for "latest state" topics;
- expose application readiness only after required listeners are registered and the application is
  ready to refetch state;
- avoid treating `ZmqFluxMqSubscriberContainer.isRunning()` as proof that every remote publisher or
  subscriber is connected.

## Observability Example

Applications can bridge `FluxMqObservationHandler` into their metrics or tracing backend without
FluxMQ depending on that backend:

```java
@Bean
FluxMqObservationHandler fluxMqObservationHandler(MeterRegistry meterRegistry) {
  return new FluxMqObservationHandler() {
    @Override
    public void published(FluxMqEnvelope envelope, Duration duration) {
      meterRegistry.counter("fluxmq.publish", "topic", envelope.topic(), "result", "success")
          .increment();
      meterRegistry.timer("fluxmq.publish.duration", "topic", envelope.topic()).record(duration);
    }

    @Override
    public void publishFailed(String topic, Throwable error, Duration duration) {
      meterRegistry.counter("fluxmq.publish", "topic", topic, "result", "failure").increment();
    }

    @Override
    public void listenerSucceeded(
        FluxMqEnvelope envelope, String listener, int attempt, Duration duration) {
      meterRegistry.counter("fluxmq.listener", "topic", envelope.topic(), "result", "success")
          .increment();
    }

    @Override
    public void listenerFailed(
        FluxMqEnvelope envelope, String listener, int attempts, Throwable error, Duration duration) {
      meterRegistry.counter("fluxmq.listener", "topic", envelope.topic(), "result", "failure")
          .increment();
    }
  };
}
```

The example uses Micrometer-style names because Spring Boot applications commonly have Micrometer
available. FluxMQ intentionally does not depend on Micrometer.

## Message Size And Payload Design

FluxMQ should carry compact event payloads, not large documents or files. Prefer identifiers,
versions, timestamps, and small summaries that let subscribers fetch current state from the durable
system.

Recommended payload shape:

```java
public record ProductChanged(UUID productId, long version, Instant changedAt) {}
```

Avoid payloads that require every event to be received to reconstruct state. Versioned payloads let
subscribers ignore stale events and repair missed events by reading the latest version.

## Backpressure And Throughput

The publisher uses a bounded queue between application threads and the PUB socket worker. A publish
call serializes the payload, enqueues a command, waits for the worker send to complete, and fails if
the queue cannot accept the command before `fluxmq.publisher.enqueue-timeout`.

Production guidance:

- keep `queue-capacity` large enough for normal bursts;
- keep `enqueue-timeout` short enough that callers do not appear hung during overload;
- count publish failures and queue-full errors through `FluxMqObservationHandler`;
- benchmark with representative payloads, topic counts, subscribers, and network topology;
- prefer dropping or coalescing upstream advisory signals over letting critical request threads pile
  up behind non-critical notifications.

FluxMQ does not currently provide a built-in benchmark harness. A production benchmark should measure
publish latency, publish failures, subscriber handler latency, process CPU, and message loss under
subscriber startup, slow subscriber, and burst scenarios.

## Concrete Production Pattern: Cache Invalidation

```java
public final class ProductTopics {
  public static final FluxMqTopic<ProductChanged> PRODUCT_CHANGED =
      FluxMqTopic.of("catalog.product.changed", ProductChanged.class);

  private ProductTopics() {}
}

@Service
public final class ProductService {
  private final ProductRepository products;
  private final FluxMqPublisher publisher;

  public void updateProduct(UUID productId, ProductUpdate update) {
    Product product = products.save(productId, update);
    publisher.publish(
        ProductTopics.PRODUCT_CHANGED,
        new ProductChanged(product.id(), product.version(), product.updatedAt()));
  }
}

@Component
public final class ProductCacheInvalidator {
  private final ProductCache cache;

  @FluxMqListener("catalog.product.changed")
  public void onProductChanged(ProductChanged event) {
    cache.evictIfVersionOlderThan(event.productId(), event.version());
  }
}
```

If the message is missed, cache TTLs, version checks, or explicit reads from `ProductRepository`
still restore correctness.

## Production Checklist

Before relying on FluxMQ in production:

- document which events are advisory and where their source of truth lives;
- define what happens when each event is missed;
- make listeners idempotent;
- add metrics for publish failures and listener failures;
- set queue capacity and enqueue timeout deliberately;
- keep payloads compact and versioned;
- test startup ordering and subscriber downtime;
- test serialization failures and malformed payloads;
- decide whether a periodic reconciliation loop is required;
- use a broker instead when the event is the durable handoff.
