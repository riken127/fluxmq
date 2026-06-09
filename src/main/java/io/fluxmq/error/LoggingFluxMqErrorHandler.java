package io.fluxmq.error;

import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logs listener errors and lets the subscriber continue. */
public final class LoggingFluxMqErrorHandler implements FluxMqErrorHandler {
  private static final Logger logger = LoggerFactory.getLogger(LoggingFluxMqErrorHandler.class);

  @Override
  public void handle(Throwable error, FluxMqEnvelope envelope) {
    logger.warn("FluxMQ listener failed for topic {}", envelope.topic(), error);
  }
}
