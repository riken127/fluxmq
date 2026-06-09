package io.fluxmq;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable metadata headers carried with a FluxMQ event. */
public final class FluxMqHeaders {
  public static final String CORRELATION_ID = "correlation-id";
  public static final String CAUSATION_ID = "causation-id";
  public static final String CONTENT_TYPE = "content-type";

  private static final FluxMqHeaders EMPTY = new FluxMqHeaders(Map.of());

  private final Map<String, String> values;

  private FluxMqHeaders(Map<String, String> values) {
    this.values = Map.copyOf(values);
  }

  /** Returns empty headers. */
  public static FluxMqHeaders empty() {
    return EMPTY;
  }

  /** Copies the provided headers into an immutable FluxMQ header value. */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static FluxMqHeaders of(Map<String, String> values) {
    Objects.requireNonNull(values, "values");
    if (values.isEmpty()) {
      return EMPTY;
    }
    LinkedHashMap<String, String> copy = new LinkedHashMap<>();
    values.forEach(
        (key, value) -> copy.put(requireText(key, "header key"), requireText(value, key)));
    return new FluxMqHeaders(copy);
  }

  /** Starts a new headers builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the correlation id, when present. */
  public Optional<String> correlationId() {
    return get(CORRELATION_ID);
  }

  /** Returns the causation id, when present. */
  public Optional<String> causationId() {
    return get(CAUSATION_ID);
  }

  /** Returns the content type, when present. */
  public Optional<String> contentType() {
    return get(CONTENT_TYPE);
  }

  /** Looks up a header value by key. */
  public Optional<String> get(String key) {
    return Optional.ofNullable(values.get(requireText(key, "key")));
  }

  /** Returns an immutable map view of all headers. */
  @JsonValue
  public Map<String, String> asMap() {
    return values;
  }

  /** Returns new headers with a single added or replaced value. */
  public FluxMqHeaders with(String key, String value) {
    LinkedHashMap<String, String> copy = new LinkedHashMap<>(values);
    copy.put(requireText(key, "key"), requireText(value, key));
    return new FluxMqHeaders(copy);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof FluxMqHeaders that && values.equals(that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  @Override
  public String toString() {
    return "FluxMqHeaders" + values;
  }

  /** Builder for immutable FluxMQ headers. */
  public static final class Builder {
    private final LinkedHashMap<String, String> values = new LinkedHashMap<>();

    private Builder() {}

    /** Sets the correlation id. */
    public Builder correlationId(String correlationId) {
      return header(CORRELATION_ID, correlationId);
    }

    /** Sets the causation id. */
    public Builder causationId(String causationId) {
      return header(CAUSATION_ID, causationId);
    }

    /** Sets the content type. */
    public Builder contentType(String contentType) {
      return header(CONTENT_TYPE, contentType);
    }

    /** Adds an arbitrary header. */
    public Builder header(String key, String value) {
      values.put(requireText(key, "key"), requireText(value, key));
      return this;
    }

    /** Builds immutable headers. */
    public FluxMqHeaders build() {
      return FluxMqHeaders.of(values);
    }
  }
}
