package io.fluxmq;

/** Handles listener failures without stopping the subscriber loop. */
public interface FluxMqErrorHandler {

  /** Handles an error raised while processing the given envelope. */
  void handle(Throwable error, FluxMqEnvelope envelope);
}
