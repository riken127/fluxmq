package io.fluxmq.example;

import io.fluxmq.FluxMqHeaders;
import io.fluxmq.FluxMqPublisher;
import java.util.UUID;

public final class OrderService {
  private final FluxMqPublisher publisher;

  public OrderService(FluxMqPublisher publisher) {
    this.publisher = publisher;
  }

  public void createOrder(UUID orderId) {
    FluxMqHeaders headers =
        FluxMqHeaders.builder().correlationId(orderId.toString()).contentType("application/json").build();
    publisher.publish("orders.created", new OrderCreated(orderId), headers);
  }
}
