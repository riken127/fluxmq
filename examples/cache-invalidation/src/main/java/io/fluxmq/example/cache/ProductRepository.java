package io.fluxmq.example.cache;

import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public final class ProductRepository {
  private final Map<UUID, Product> products = new ConcurrentHashMap<>();

  public Product create(UUID productId, String name) {
    Product product = new Product(productId, name, 1, Instant.now());
    Product previous = products.putIfAbsent(productId, product);
    if (previous != null) {
      throw new IllegalStateException("product already exists: " + productId);
    }
    return product;
  }

  public Product rename(UUID productId, String name) {
    return products.compute(
        productId,
        (ignored, product) -> {
          if (product == null) {
            throw new NoSuchElementException("unknown product: " + productId);
          }
          return product.rename(name);
        });
  }

  public Product get(UUID productId) {
    Product product = products.get(productId);
    if (product == null) {
      throw new NoSuchElementException("unknown product: " + productId);
    }
    return product;
  }
}
