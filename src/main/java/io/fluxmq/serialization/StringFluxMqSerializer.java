package io.fluxmq.serialization;

import io.fluxmq.FluxMqSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** UTF-8 serializer for plain string payloads. */
public final class StringFluxMqSerializer implements FluxMqSerializer {

  @Override
  public byte[] serialize(Object value) {
    Objects.requireNonNull(value, "value");
    if (value instanceof String text) {
      return text.getBytes(StandardCharsets.UTF_8);
    }
    throw new IllegalArgumentException("String FluxMQ serializer only supports String payloads");
  }

  @Override
  public <T> T deserialize(byte[] bytes, Class<T> targetType) {
    Objects.requireNonNull(bytes, "bytes");
    Objects.requireNonNull(targetType, "targetType");
    if (String.class.equals(targetType)) {
      return targetType.cast(new String(bytes, StandardCharsets.UTF_8));
    }
    throw new IllegalArgumentException("String FluxMQ serializer only supports String targets");
  }
}
