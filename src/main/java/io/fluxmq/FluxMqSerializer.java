package io.fluxmq;

/** Serializes and deserializes FluxMQ payloads. */
public interface FluxMqSerializer {

  /** Serializes a payload value. */
  byte[] serialize(Object value);

  /** Deserializes payload bytes into the requested target type. */
  <T> T deserialize(byte[] bytes, Class<T> targetType);
}
