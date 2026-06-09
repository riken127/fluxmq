package io.fluxmq.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.fluxmq.autoconfigure.FluxMqProperties.EndpointMode;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

final class FluxMqPropertiesTest {

  @Test
  void resolvesBindEndpoint() {
    FluxMqProperties properties = new FluxMqProperties();
    properties.getPublisher().setBind("tcp://*:5556");

    assertThat(properties.getPublisher().requireEndpoint("publisher").mode())
        .isEqualTo(EndpointMode.BIND);
    assertThat(properties.getPublisher().requireEndpoint("publisher").endpoint())
        .isEqualTo("tcp://*:5556");
  }

  @Test
  void copiesSubscriberTopics() {
    FluxMqProperties properties = new FluxMqProperties();
    properties.getSubscriber().setTopics(List.of("orders.created"));

    assertThat(properties.getSubscriber().getTopics()).containsExactly("orders.created");
  }

  @Test
  void acceptsNonJsonSerializerFormat() {
    FluxMqProperties properties = new FluxMqProperties();

    properties.getSerialization().setFormat("string");

    assertThat(properties.getSerialization().getFormat()).isEqualTo("string");
  }

  @Test
  void rejectsNonPositiveShutdownTimeout() {
    FluxMqProperties properties = new FluxMqProperties();

    assertThatThrownBy(() -> properties.getShutdown().setTimeout(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("fluxmq.shutdown.timeout must be positive");
  }

  @Test
  void rejectsInvalidPublisherQueueCapacity() {
    FluxMqProperties properties = new FluxMqProperties();

    assertThatThrownBy(() -> properties.getPublisher().setQueueCapacity(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("fluxmq.publisher.queue-capacity must be positive");
  }

  @Test
  void rejectsNegativePublisherEnqueueTimeout() {
    FluxMqProperties properties = new FluxMqProperties();

    assertThatThrownBy(() -> properties.getPublisher().setEnqueueTimeout(Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("fluxmq.publisher.enqueue-timeout must not be negative");
  }

  @Test
  void rejectsInvalidRetryAttempts() {
    FluxMqProperties properties = new FluxMqProperties();

    assertThatThrownBy(() -> properties.getRetry().setMaxAttempts(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("fluxmq.retry.max-attempts must be positive");
  }
}
