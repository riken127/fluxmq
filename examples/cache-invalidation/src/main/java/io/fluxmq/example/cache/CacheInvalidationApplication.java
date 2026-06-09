package io.fluxmq.example.cache;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CacheInvalidationApplication {
  private static final Logger logger =
      LoggerFactory.getLogger(CacheInvalidationApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(CacheInvalidationApplication.class, args);
  }

  @Bean
  @ConditionalOnProperty(
      prefix = "example.cache-invalidation",
      name = "run-on-startup",
      havingValue = "true",
      matchIfMissing = true)
  ApplicationRunner cacheInvalidationDemo(
      ProductRepository repository, ProductCache cache, ProductService products) {
    return ignored -> {
      UUID productId = UUID.randomUUID();
      repository.create(productId, "Trail Camera");
      Product firstRead = cache.get(productId);

      Thread.sleep(500);
      Product updated = products.renameProduct(productId, "Trail Camera Pro");

      logger.info("first cache read: {}", firstRead);
      logger.info("repository update: {}", updated);
      logger.info("next cache read after invalidation hint: {}", cache.get(productId));
    };
  }
}
