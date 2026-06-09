package io.fluxmq.example.cache;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ProductChanged(UUID productId, long version, Instant changedAt) {

  public ProductChanged {
    Objects.requireNonNull(productId, "productId");
    if (version <= 0) {
      throw new IllegalArgumentException("version must be positive");
    }
    Objects.requireNonNull(changedAt, "changedAt");
  }

  static ProductChanged from(Product product) {
    return new ProductChanged(product.id(), product.version(), product.updatedAt());
  }
}
