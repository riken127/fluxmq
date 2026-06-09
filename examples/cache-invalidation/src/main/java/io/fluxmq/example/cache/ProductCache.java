package io.fluxmq.example.cache;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public final class ProductCache {
  private final ProductRepository repository;
  private final Map<UUID, CachedProduct> cachedProducts = new ConcurrentHashMap<>();

  public ProductCache(ProductRepository repository) {
    this.repository = repository;
  }

  public Product get(UUID productId) {
    return cachedProducts.computeIfAbsent(productId, this::load).product();
  }

  public boolean hasCachedProduct(UUID productId) {
    return cachedProducts.containsKey(productId);
  }

  public void evictIfVersionOlderThan(UUID productId, long version) {
    cachedProducts.computeIfPresent(
        productId, (ignored, cached) -> cached.product().version() < version ? null : cached);
  }

  private CachedProduct load(UUID productId) {
    return new CachedProduct(repository.get(productId), Instant.now());
  }

  private record CachedProduct(Product product, Instant cachedAt) {}
}
