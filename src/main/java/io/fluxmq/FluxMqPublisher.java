package io.fluxmq;

/** Publishes best-effort events to FluxMQ topics. */
public interface FluxMqPublisher {

  /** Publishes a payload with empty headers. */
  void publish(String topic, Object payload);

  /** Publishes a payload with immutable FluxMQ headers. */
  void publish(String topic, Object payload, FluxMqHeaders headers);
}
