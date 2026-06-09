package io.fluxmq.example;

import io.fluxmq.FluxMqHeaders;
import io.fluxmq.FluxMqListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public final class OrderEventHandler {
  private static final Logger logger = LoggerFactory.getLogger(OrderEventHandler.class);

  @FluxMqListener("orders.created")
  public void onOrderCreated(OrderCreated event, FluxMqHeaders headers) {
    logger.info("received order event {} with headers {}", event.orderId(), headers.asMap());
  }
}
