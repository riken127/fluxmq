package io.fluxmq.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class FluxMqSerializerRegistryTest {

  @Test
  void selectsRegisteredSerializerByFormat() {
    FluxMqSerializerRegistry registry =
        FluxMqSerializerRegistry.builder()
            .serializer("string", new StringFluxMqSerializer())
            .build();

    assertThat(
            registry
                .serializer("STRING")
                .deserialize("hello".getBytes(StandardCharsets.UTF_8), String.class))
        .isEqualTo("hello");
  }

  @Test
  void rejectsUnknownSerializerFormat() {
    FluxMqSerializerRegistry registry =
        FluxMqSerializerRegistry.builder()
            .serializer("bytes", new ByteArrayFluxMqSerializer())
            .build();

    assertThatThrownBy(() -> registry.serializer("json"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unsupported FluxMQ serialization format json");
  }
}
