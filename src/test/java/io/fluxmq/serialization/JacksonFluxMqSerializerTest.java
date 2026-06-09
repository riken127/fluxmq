package io.fluxmq.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

final class JacksonFluxMqSerializerTest {

  @Test
  void roundTripsJsonPayload() {
    JacksonFluxMqSerializer serializer = new JacksonFluxMqSerializer(new ObjectMapper());

    byte[] bytes = serializer.serialize(new OrderCreated("order-1"));

    assertThat(serializer.deserialize(bytes, OrderCreated.class))
        .isEqualTo(new OrderCreated("order-1"));
  }

  record OrderCreated(String id) {}
}
