package io.fluxmq;

import java.util.Objects;

/** Publishes best-effort events to FluxMQ topics. */
public interface FluxMqPublisher {

  /** Publishes a typed topic payload with empty headers. */
  default <T> void publish(FluxMqTopic<T> topic, T payload) {
    publish(topic, payload, FluxMqHeaders.empty());
  }

  /** Publishes a typed topic payload with immutable FluxMQ headers. */
  default <T> void publish(FluxMqTopic<T> topic, T payload, FluxMqHeaders headers) {
    FluxMqTopic<T> resolvedTopic = Objects.requireNonNull(topic, "topic");
    T resolvedPayload =
        resolvedTopic.payloadType().cast(Objects.requireNonNull(payload, "payload"));
    publish(resolvedTopic.name(), resolvedPayload, headers);
  }

  /** Publishes a payload with empty headers. */
  void publish(String topic, Object payload);

  /** Publishes a payload with immutable FluxMQ headers. */
  void publish(String topic, Object payload, FluxMqHeaders headers);
}
