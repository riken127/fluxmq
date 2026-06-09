package io.fluxmq.serialization;

import io.fluxmq.FluxMqSerializer;
import java.util.Objects;

/** Serializer for payloads that are already byte arrays. */
public final class ByteArrayFluxMqSerializer implements FluxMqSerializer {

  @Override
  public byte[] serialize(Object value) {
    Objects.requireNonNull(value, "value");
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    throw new IllegalArgumentException(
        "Byte array FluxMQ serializer only supports byte[] payloads");
  }

  @Override
  public <T> T deserialize(byte[] bytes, Class<T> targetType) {
    Objects.requireNonNull(bytes, "bytes");
    Objects.requireNonNull(targetType, "targetType");
    if (byte[].class.equals(targetType)) {
      return targetType.cast(bytes.clone());
    }
    throw new IllegalArgumentException("Byte array FluxMQ serializer only supports byte[] targets");
  }
}
