# FluxMQ Messaging Semantics

## Delivery Model

FluxMQ uses ZeroMQ PUB/SUB through JeroMQ. Delivery is best-effort and in-memory. A publisher sends a
multipart message with topic, envelope metadata JSON, and payload bytes. A subscriber dispatches the
message to listeners registered for the exact topic.

## Ordering Assumptions

Messages sent by one publisher socket to one subscriber socket are observed in socket order when
they are delivered. FluxMQ does not define global ordering across publishers, processes, sockets, or
topics.

## Failure Scenarios

Messages can be lost when:

- a subscriber is not connected;
- a subscription has not reached the publisher yet;
- a subscriber is slow;
- a process crashes;
- a socket is closed before messages are observed;
- the network drops traffic.

Listener exceptions are passed to `FluxMqErrorHandler`. The subscriber loop continues after handler
failure.

When `fluxmq.retry.max-attempts` is greater than `1`, FluxMQ retries listener invocation locally on
the subscriber thread. This is not transport redelivery and does not make delivery durable.

## Startup Ordering

PUB/SUB has a startup race. Subscribers need time to connect and subscribe before publishers send.
For important events, coordinate startup externally or use a broker with durable delivery.

## Dropped Messages

FluxMQ does not detect or replay dropped messages. Applications that need stronger behavior should
add their own persistence/idempotency layer or choose a broker.

## Idempotency Recommendation

Handlers should be idempotent for meaningful business events. Include stable identifiers in payloads
or headers so duplicate external publishes can be handled safely by the application.

## When Not To Use FluxMQ

Do not use FluxMQ when lost messages are unacceptable, when consumers must catch up after downtime,
or when acknowledgement, replay, partitioning, consumer groups, distributed transactions, or
operational broker tooling are required.
