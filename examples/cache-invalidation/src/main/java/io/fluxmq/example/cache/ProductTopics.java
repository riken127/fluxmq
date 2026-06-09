package io.fluxmq.example.cache;

import io.fluxmq.FluxMqTopic;

public final class ProductTopics {
  public static final String PRODUCT_CHANGED_TOPIC = "catalog.product.changed";
  public static final FluxMqTopic<ProductChanged> PRODUCT_CHANGED =
      FluxMqTopic.of(PRODUCT_CHANGED_TOPIC, ProductChanged.class);

  private ProductTopics() {}
}
