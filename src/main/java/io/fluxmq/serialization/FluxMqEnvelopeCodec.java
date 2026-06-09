package io.fluxmq.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqHeaders;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/** Encodes and decodes the FluxMQ envelope metadata frame. */
public final class FluxMqEnvelopeCodec {
  private final ObjectMapper objectMapper;

  /** Creates a codec backed by the provided object mapper. */
  public FluxMqEnvelopeCodec(ObjectMapper objectMapper) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  /** Encodes envelope metadata without the payload bytes. */
  public byte[] encodeMetadata(FluxMqEnvelope envelope) {
    Objects.requireNonNull(envelope, "envelope");
    Metadata metadata =
        new Metadata(
            envelope.id(),
            envelope.topic(),
            envelope.type(),
            envelope.timestamp(),
            envelope.headers());
    try {
      return objectMapper.writeValueAsBytes(metadata);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Failed to encode FluxMQ envelope metadata", exception);
    }
  }

  /** Decodes metadata and combines it with the payload frame. */
  public FluxMqEnvelope decode(String frameTopic, byte[] metadataBytes, byte[] payloadBytes) {
    Objects.requireNonNull(frameTopic, "frameTopic");
    Objects.requireNonNull(metadataBytes, "metadataBytes");
    Objects.requireNonNull(payloadBytes, "payloadBytes");
    try {
      Metadata metadata = objectMapper.readValue(metadataBytes, Metadata.class);
      if (!frameTopic.equals(metadata.topic())) {
        throw new IllegalArgumentException(
            "FluxMQ topic frame "
                + frameTopic
                + " did not match envelope topic "
                + metadata.topic());
      }
      return new FluxMqEnvelope(
          metadata.id(),
          metadata.topic(),
          metadata.type(),
          metadata.timestamp(),
          metadata.headers(),
          payloadBytes);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to decode FluxMQ envelope metadata", exception);
    }
  }

  private record Metadata(
      String id, String topic, String type, Instant timestamp, FluxMqHeaders headers) {}
}
