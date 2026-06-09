package io.fluxmq.autoconfigure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for FluxMQ Spring Boot auto-configuration. */
@ConfigurationProperties(prefix = "fluxmq")
public class FluxMqProperties {
  private boolean enabled = true;
  private final Publisher publisher = new Publisher();
  private final Subscriber subscriber = new Subscriber();
  private final Shutdown shutdown = new Shutdown();
  private final Serialization serialization = new Serialization();
  private final Retry retry = new Retry();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Publisher getPublisher() {
    return publisher;
  }

  public Subscriber getSubscriber() {
    return subscriber;
  }

  public Shutdown getShutdown() {
    return shutdown;
  }

  public Serialization getSerialization() {
    return serialization;
  }

  public Retry getRetry() {
    return retry;
  }

  /** Publisher or subscriber ZeroMQ endpoint settings. */
  public static class Endpoint {
    private String bind;
    private String connect;

    /** Returns the configured bind endpoint, when present. */
    public Optional<String> bind() {
      return Optional.ofNullable(textOrNull(bind));
    }

    /** Sets the endpoint this role should bind. */
    public void setBind(String bind) {
      this.bind = bind;
    }

    /** Returns the configured connect endpoint, when present. */
    public Optional<String> connect() {
      return Optional.ofNullable(textOrNull(connect));
    }

    /** Sets the endpoint this role should connect. */
    public void setConnect(String connect) {
      this.connect = connect;
    }

    /** Returns true when either bind or connect is configured. */
    public boolean configured() {
      validateNoAmbiguousEndpoint("endpoint");
      return bind().isPresent() || connect().isPresent();
    }

    /** Resolves this endpoint for an active role, or throws a clear configuration error. */
    public ResolvedEndpoint requireEndpoint(String role) {
      validateNoAmbiguousEndpoint(role);
      Optional<String> bindEndpoint = bind();
      Optional<String> connectEndpoint = connect();
      if (bindEndpoint.isPresent()) {
        return new ResolvedEndpoint(EndpointMode.BIND, bindEndpoint.get());
      }
      if (connectEndpoint.isPresent()) {
        return new ResolvedEndpoint(EndpointMode.CONNECT, connectEndpoint.get());
      }
      throw new IllegalStateException("fluxmq." + role + " must configure either bind or connect");
    }

    private void validateNoAmbiguousEndpoint(String role) {
      if (bind().isPresent() && connect().isPresent()) {
        throw new IllegalStateException(
            "fluxmq." + role + " must not configure both bind and connect");
      }
    }
  }

  /** Publisher endpoint and worker queue settings. */
  public static final class Publisher extends Endpoint {
    private int queueCapacity = 1024;
    private Duration enqueueTimeout = Duration.ofSeconds(5);

    public int getQueueCapacity() {
      return queueCapacity;
    }

    /** Sets the bounded publisher worker queue capacity. */
    public void setQueueCapacity(int queueCapacity) {
      if (queueCapacity <= 0) {
        throw new IllegalArgumentException("fluxmq.publisher.queue-capacity must be positive");
      }
      this.queueCapacity = queueCapacity;
    }

    public Duration getEnqueueTimeout() {
      return enqueueTimeout;
    }

    /** Sets how long publish waits for space in the worker queue. */
    public void setEnqueueTimeout(Duration enqueueTimeout) {
      this.enqueueTimeout = Objects.requireNonNull(enqueueTimeout, "enqueueTimeout");
      if (enqueueTimeout.isNegative()) {
        throw new IllegalArgumentException("fluxmq.publisher.enqueue-timeout must not be negative");
      }
    }
  }

  /** Subscriber endpoint and topic settings. */
  public static final class Subscriber extends Endpoint {
    private List<String> topics = new ArrayList<>();

    public List<String> getTopics() {
      return List.copyOf(topics);
    }

    public void setTopics(List<String> topics) {
      this.topics = new ArrayList<>(Objects.requireNonNull(topics, "topics"));
    }
  }

  /** Shutdown behavior for managed FluxMQ components. */
  public static final class Shutdown {
    private Duration timeout = Duration.ofSeconds(5);

    public Duration getTimeout() {
      return timeout;
    }

    /** Sets the graceful shutdown timeout. */
    public void setTimeout(Duration timeout) {
      this.timeout = Objects.requireNonNull(timeout, "timeout");
      if (timeout.isNegative() || timeout.isZero()) {
        throw new IllegalArgumentException("fluxmq.shutdown.timeout must be positive");
      }
    }
  }

  /** Serialization settings. */
  public static final class Serialization {
    private String format = "json";

    public String getFormat() {
      return format;
    }

    /** Sets the serialization format. Built-in formats are json, string, and bytes. */
    public void setFormat(String format) {
      this.format = requireText(format, "fluxmq.serialization.format");
    }
  }

  /** Local listener retry settings. */
  public static final class Retry {
    private int maxAttempts = 1;
    private Duration delay = Duration.ZERO;

    public int getMaxAttempts() {
      return maxAttempts;
    }

    /** Sets local listener attempts. A value of 1 disables retry. */
    public void setMaxAttempts(int maxAttempts) {
      if (maxAttempts <= 0) {
        throw new IllegalArgumentException("fluxmq.retry.max-attempts must be positive");
      }
      this.maxAttempts = maxAttempts;
    }

    public Duration getDelay() {
      return delay;
    }

    /** Sets delay between local listener retry attempts. */
    public void setDelay(Duration delay) {
      this.delay = Objects.requireNonNull(delay, "delay");
      if (delay.isNegative()) {
        throw new IllegalArgumentException("fluxmq.retry.delay must not be negative");
      }
    }
  }

  /** Endpoint mode for a ZeroMQ socket. */
  public enum EndpointMode {
    BIND,
    CONNECT
  }

  /** Resolved endpoint with exactly one bind or connect mode. */
  public record ResolvedEndpoint(EndpointMode mode, String endpoint) {
    /** Creates a resolved endpoint. */
    public ResolvedEndpoint {
      endpoint = requireText(endpoint, "endpoint");
    }
  }

  private static String textOrNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
