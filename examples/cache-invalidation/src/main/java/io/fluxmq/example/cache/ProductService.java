package io.fluxmq.example.cache;

import io.fluxmq.FluxMqPublisher;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public final class ProductService {
  private final ProductRepository repository;
  private final FluxMqPublisher publisher;

  public ProductService(ProductRepository repository, FluxMqPublisher publisher) {
    this.repository = repository;
    this.publisher = publisher;
  }

  public Product renameProduct(UUID productId, String name) {
    Product product = repository.rename(productId, name);
    publisher.publish(ProductTopics.PRODUCT_CHANGED, ProductChanged.from(product));
    return product;
  }
}
