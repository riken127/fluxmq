package io.fluxmq.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqHeaders;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class FluxMqEnvelopeCodecTest {
  private final FluxMqEnvelopeCodec codec =
      new FluxMqEnvelopeCodec(new ObjectMapper().registerModule(new JavaTimeModule()));

  @Test
  void roundTripsEnvelopeMetadata() {
    FluxMqEnvelope envelope =
        new FluxMqEnvelope(
            "event-1",
            "orders.created",
            OrderCreated.class.getName(),
            Instant.parse("2026-01-01T00:00:00Z"),
            FluxMqHeaders.builder().contentType("application/json").build(),
            new byte[] {1, 2, 3});

    FluxMqEnvelope decoded =
        codec.decode("orders.created", codec.encodeMetadata(envelope), new byte[] {1, 2, 3});

    assertThat(decoded).isEqualTo(envelope);
  }

  @Test
  void rejectsTopicMismatch() {
    FluxMqEnvelope envelope =
        new FluxMqEnvelope(
            "event-1",
            "orders.created",
            OrderCreated.class.getName(),
            Instant.parse("2026-01-01T00:00:00Z"),
            FluxMqHeaders.empty(),
            new byte[] {1});

    assertThatThrownBy(
            () ->
                codec.decode("payments.completed", codec.encodeMetadata(envelope), new byte[] {1}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("did not match");
  }

  record OrderCreated(String id) {}
}
