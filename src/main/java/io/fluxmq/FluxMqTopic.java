package io.fluxmq;

import java.util.Objects;

/** A FluxMQ topic name paired with the payload type callers should publish on it. */
public record FluxMqTopic<T>(String name, Class<T> payloadType) {

  /** Creates a typed topic. */
  public FluxMqTopic {
    name = requireText(name, "name");
    payloadType = Objects.requireNonNull(payloadType, "payloadType");
  }

  /** Creates a typed topic for payloads of {@code payloadType}. */
  public static <T> FluxMqTopic<T> of(String name, Class<T> payloadType) {
    return new FluxMqTopic<>(name, payloadType);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
