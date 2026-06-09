package io.fluxmq;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/** Serialized event envelope exchanged by FluxMQ transport code. */
public record FluxMqEnvelope(
    String id,
    String topic,
    String type,
    Instant timestamp,
    FluxMqHeaders headers,
    byte[] payload) {

  /** Creates an immutable envelope and defensively copies payload bytes. */
  public FluxMqEnvelope {
    id = requireText(id, "id");
    topic = requireText(topic, "topic");
    type = requireText(type, "type");
    timestamp = Objects.requireNonNull(timestamp, "timestamp");
    headers = Objects.requireNonNull(headers, "headers");
    payload = Objects.requireNonNull(payload, "payload").clone();
  }

  @Override
  public byte[] payload() {
    return payload.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FluxMqEnvelope that)) {
      return false;
    }
    return id.equals(that.id)
        && topic.equals(that.topic)
        && type.equals(that.type)
        && timestamp.equals(that.timestamp)
        && headers.equals(that.headers)
        && Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, topic, type, timestamp, headers);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return "FluxMqEnvelope["
        + "id="
        + id
        + ", topic="
        + topic
        + ", type="
        + type
        + ", timestamp="
        + timestamp
        + ", headers="
        + headers
        + ", payloadLength="
        + payload.length
        + ']';
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
