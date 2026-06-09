package io.fluxmq.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fluxmq.FluxMqSerializer;
import java.io.IOException;
import java.util.Objects;

/** Jackson JSON implementation of FluxMQ payload serialization. */
public final class JacksonFluxMqSerializer implements FluxMqSerializer {
  private final ObjectMapper objectMapper;

  /** Creates a Jackson-backed payload serializer. */
  public JacksonFluxMqSerializer(ObjectMapper objectMapper) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public byte[] serialize(Object value) {
    Objects.requireNonNull(value, "value");
    try {
      return objectMapper.writeValueAsBytes(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Failed to serialize FluxMQ payload", exception);
    }
  }

  @Override
  public <T> T deserialize(byte[] bytes, Class<T> targetType) {
    Objects.requireNonNull(bytes, "bytes");
    Objects.requireNonNull(targetType, "targetType");
    try {
      return objectMapper.readValue(bytes, targetType);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to deserialize FluxMQ payload", exception);
    }
  }
}
