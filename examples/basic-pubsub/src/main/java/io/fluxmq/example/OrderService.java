package io.fluxmq.example;

import io.fluxmq.FluxMqHeaders;
import io.fluxmq.FluxMqPublisher;
import io.fluxmq.FluxMqTopic;
import java.util.UUID;

public final class OrderService {
  private static final FluxMqTopic<OrderCreated> ORDERS_CREATED =
      FluxMqTopic.of("orders.created", OrderCreated.class);

  private final FluxMqPublisher publisher;

  public OrderService(FluxMqPublisher publisher) {
    this.publisher = publisher;
  }

  public void createOrder(UUID orderId) {
    FluxMqHeaders headers =
        FluxMqHeaders.builder().correlationId(orderId.toString()).contentType("application/json").build();
    publisher.publish(ORDERS_CREATED, new OrderCreated(orderId), headers);
  }
}
