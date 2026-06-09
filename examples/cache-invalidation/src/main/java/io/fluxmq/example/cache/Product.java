package io.fluxmq.example.cache;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record Product(UUID id, String name, long version, Instant updatedAt) {

  public Product {
    Objects.requireNonNull(id, "id");
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (version <= 0) {
      throw new IllegalArgumentException("version must be positive");
    }
    Objects.requireNonNull(updatedAt, "updatedAt");
  }

  Product rename(String newName) {
    return new Product(id, newName, version + 1, Instant.now());
  }
}
