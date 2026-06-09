# FluxMQ Production Use Cases

FluxMQ is for production systems that need lightweight, low-overhead, best-effort event fanout and
can recover correctness from another source of truth. It is not a broker, but that does not make it
only a toy: many production messages are hints, snapshots, status signals, or invalidations where
freshness matters more than guaranteed delivery of every intermediate event.

The central question is: "If this message is missed, can the subscriber still become correct by
waiting for the next message or reading current state elsewhere?" If yes, FluxMQ can be a good fit.

See [Production Readiness](production-readiness.md) for lifecycle, observability, backpressure, and
recovery guidance before using these patterns in production.

## Production Fit Criteria

FluxMQ is a strong candidate when:

- events are advisory, not authoritative;
- the database, filesystem, device, cache origin, or upstream service remains the source of truth;
- consumers can tolerate missing messages and converge later;
- low latency and a small runtime footprint matter;
- the topology is simple enough for exact string topics;
- the deployment is within a trusted network, host, cluster, or edge environment;
- operating a broker would be disproportionate to the value of the signal.

## Production Use Cases

### Cache Invalidation And Refresh Hints

Use FluxMQ to notify services that cached data may be stale. Missing an invalidation should not
corrupt the system because consumers can still use TTLs, version checks, or explicit reads from the
source of truth.

Examples:

- publish `catalog.product.changed` after product data changes so API nodes refresh hot entries;
- publish `tenant.settings.changed` so workers reload tenant configuration on demand;
- publish `permissions.changed` so authorization caches evict affected principals;
- publish `document.updated` so preview or indexing services refetch the document.

This is a good production use because the message is a nudge. The durable state lives elsewhere.

### Read-Model And Search-Index Refresh

Use FluxMQ to trigger best-effort refreshes of derived views when the authoritative model changes.
The derived system should be able to rebuild from the primary store or reconcile periodically.

Examples:

- notify a search service that a product, document, or profile should be re-indexed;
- notify a reporting projection that an account summary should be recalculated;
- notify a recommendation cache that a user's preferences changed;
- notify a denormalized read API that a record should be fetched again.

FluxMQ should not be the only record of the change. It works well when it accelerates convergence
and a scheduled repair or direct lookup preserves correctness.

### Control-Plane Broadcasts

Use FluxMQ for small operational control-plane signals that many services may observe, where the
latest state is more important than every transition.

Examples:

- feature flag version changed;
- rate-limit policy changed;
- routing table or shard map refreshed;
- maintenance mode enabled or disabled;
- service capability list changed.

Subscribers that miss a broadcast can recover by polling the control-plane API, reading a versioned
configuration record, or receiving the next broadcast.

### Live Dashboards And Operator Screens

Use FluxMQ to feed live operational views where dropped updates are acceptable because a later update
replaces the previous one.

Examples:

- background job progress;
- deployment stage changes;
- queue depth snapshots from another system;
- active session counts;
- node health summaries;
- operator notifications that are also visible in durable logs or an admin API.

The UI should treat FluxMQ as a live update channel, not as the historical audit source.

### Edge Gateway And Local Network Telemetry

Use FluxMQ in edge, lab, factory, kiosk, robotics, or device-adjacent deployments where local
processes need fast fanout without running a broker. Telemetry consumers should work from the latest
sample and tolerate gaps.

Examples:

- device temperature, voltage, or signal-quality snapshots;
- lab instrument readings;
- robot simulation state;
- kiosk peripheral status;
- gateway health and connectivity updates;
- local sensor aggregation before another system stores selected readings.

This is a practical production niche when the network is controlled and losing some samples is
acceptable.

### Low-Latency Fanout Inside A Trusted Service Boundary

Use FluxMQ when multiple in-house consumers need the same near-real-time signal and the producer
should not call each consumer directly. This avoids direct service coupling while keeping the runtime
small.

Examples:

- pricing or availability snapshots where the next snapshot supersedes the missed one;
- game server room-state updates where clients receive frequent fresh state;
- local risk, capacity, or quota snapshots backed by an authoritative service;
- live collaboration presence updates where absence can be corrected by heartbeat or resync.

Do not use this pattern for authoritative trades, payments, customer orders, audit records, or any
event that must be processed exactly once.

### Non-Critical Observability And Diagnostics

Use FluxMQ for production diagnostic streams that help operators and developers understand a running
system but are not the official metrics, tracing, or audit pipeline.

Examples:

- high-volume debug breadcrumbs during an incident;
- component lifecycle notifications;
- sampling-based performance hints;
- temporary investigation streams;
- local process events consumed by a sidecar or admin UI.

Important telemetry should still go to a durable observability backend. FluxMQ is useful for the
fast, local, disposable layer.

### Optional Module And Plugin Events

Use FluxMQ in modular Spring Boot systems where optional modules should observe application events
without becoming compile-time dependencies of the publisher.

Examples:

- a core document service emits `documents.changed`, while optional modules refresh previews,
  embeddings, or local search state;
- an administration module listens for `users.changed` to refresh active operator views;
- a plugin listens for product updates to maintain a local derived cache.

This works best when optional modules can miss an event and later rebuild from the core application
state.

## When A Broker Is Still The Better Production Choice

Choose Kafka, RabbitMQ, NATS JetStream, Pulsar, SQS, Pub/Sub, or another broker when the event itself
is the durable handoff. FluxMQ is the wrong tool when you need:

- durable queues;
- replay after downtime;
- consumer groups;
- acknowledgements or redelivery;
- dead-letter queues;
- exactly-once processing;
- transactions;
- audit trails;
- schema registry;
- broker-managed backpressure;
- operational broker tooling.

## Production Decision Examples

Good fit: "When product data changes, notify API nodes to evict local cache entries. If an API node
misses the message, its TTL expires or it can refetch by version."

Good fit: "Broadcast current device readings to local consumers. Consumers only need recent samples,
and selected readings are stored by a separate telemetry service."

Good fit: "Tell dashboard clients that a deployment moved to a new phase. The admin API remains the
source of truth for current and historical deployment state."

Poor fit: "Every invoice event must be consumed, audited, and replayed after downtime."

Poor fit: "Workers must process each job exactly once and retry failures until success."
