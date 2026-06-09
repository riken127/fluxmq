package io.fluxmq.example.cache;

import io.fluxmq.FluxMqListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public final class ProductCacheInvalidator {
  private static final Logger logger = LoggerFactory.getLogger(ProductCacheInvalidator.class);

  private final ProductCache cache;

  public ProductCacheInvalidator(ProductCache cache) {
    this.cache = cache;
  }

  @FluxMqListener(ProductTopics.PRODUCT_CHANGED_TOPIC)
  public void onProductChanged(ProductChanged event) {
    cache.evictIfVersionOlderThan(event.productId(), event.version());
    logger.info("evicted cached product {} before version {}", event.productId(), event.version());
  }
}
