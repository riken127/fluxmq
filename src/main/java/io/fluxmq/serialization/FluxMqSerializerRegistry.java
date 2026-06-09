package io.fluxmq.serialization;

import io.fluxmq.FluxMqSerializer;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Registry of named serializers exposed behind the {@link FluxMqSerializer} API. */
public final class FluxMqSerializerRegistry {
  private final Map<String, FluxMqSerializer> serializers;

  private FluxMqSerializerRegistry(Map<String, FluxMqSerializer> serializers) {
    this.serializers = Map.copyOf(serializers);
  }

  /** Creates a registry builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the serializer for a configured format. */
  public FluxMqSerializer serializer(String format) {
    FluxMqSerializer serializer = serializers.get(normalize(format));
    if (serializer == null) {
      throw new IllegalStateException(
          "Unsupported FluxMQ serialization format "
              + format
              + "; available formats are "
              + serializers.keySet());
    }
    return serializer;
  }

  /** Returns supported format names. */
  public Set<String> formats() {
    return serializers.keySet();
  }

  private static String normalize(String format) {
    Objects.requireNonNull(format, "format");
    if (format.isBlank()) {
      throw new IllegalArgumentException("format must not be blank");
    }
    return format.toLowerCase(Locale.ROOT);
  }

  /** Builder for immutable serializer registries. */
  public static final class Builder {
    private final Map<String, FluxMqSerializer> serializers = new LinkedHashMap<>();

    private Builder() {}

    /** Registers a serializer format. */
    public Builder serializer(String format, FluxMqSerializer serializer) {
      serializers.put(normalize(format), Objects.requireNonNull(serializer, "serializer"));
      return this;
    }

    /** Builds an immutable registry. */
    public FluxMqSerializerRegistry build() {
      if (serializers.isEmpty()) {
        throw new IllegalStateException("At least one FluxMQ serializer must be registered");
      }
      return new FluxMqSerializerRegistry(serializers);
    }
  }
}
