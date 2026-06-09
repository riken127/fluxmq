package io.fluxmq;

import java.time.Duration;

/** Receives lightweight FluxMQ metrics and tracing signals without requiring a backend. */
public interface FluxMqObservationHandler {

  /** Called after a message is published successfully. */
  default void published(FluxMqEnvelope envelope, Duration duration) {}

  /** Called when publishing fails before the message is accepted by the PUB socket. */
  default void publishFailed(String topic, Throwable error, Duration duration) {}

  /** Called after a listener handles an envelope successfully. */
  default void listenerSucceeded(
      FluxMqEnvelope envelope, String listener, int attempt, Duration duration) {}

  /** Called after listener handling fails after all local attempts. */
  default void listenerFailed(
      FluxMqEnvelope envelope, String listener, int attempts, Throwable error, Duration duration) {}
}
