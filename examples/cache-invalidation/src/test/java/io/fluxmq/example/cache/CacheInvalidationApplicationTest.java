package io.fluxmq.example.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "example.cache-invalidation.run-on-startup=false",
      "fluxmq.publisher.bind=inproc://cache-invalidation-test",
      "fluxmq.subscriber.connect=inproc://cache-invalidation-test"
    })
final class CacheInvalidationApplicationTest {
  @Autowired private ProductRepository repository;
  @Autowired private ProductCache cache;
  @Autowired private ProductService products;

  @Test
  void productUpdateInvalidatesStaleCacheAndNextReadFetchesLatestState() throws Exception {
    UUID productId = UUID.randomUUID();
    Product created = repository.create(productId, "Trail Camera");
    Product firstRead = cache.get(productId);

    assertThat(firstRead).isEqualTo(created);
    assertThat(cache.hasCachedProduct(productId)).isTrue();

    Thread.sleep(250);
    Product updated = products.renameProduct(productId, "Trail Camera Pro");

    await(() -> !cache.hasCachedProduct(productId));

    Product refreshed = cache.get(productId);
    assertThat(refreshed).isEqualTo(updated);
    assertThat(refreshed.name()).isEqualTo("Trail Camera Pro");
    assertThat(refreshed.version()).isEqualTo(2);
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + 2_000_000_000L;
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(condition.getAsBoolean()).isTrue();
  }
}
