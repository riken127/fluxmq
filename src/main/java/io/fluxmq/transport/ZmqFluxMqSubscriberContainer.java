package io.fluxmq.transport;

import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqErrorHandler;
import io.fluxmq.FluxMqObservationHandler;
import io.fluxmq.FluxMqSerializer;
import io.fluxmq.autoconfigure.FluxMqProperties;
import io.fluxmq.autoconfigure.FluxMqProperties.ResolvedEndpoint;
import io.fluxmq.listener.FluxMqListenerMethod;
import io.fluxmq.listener.FluxMqListenerRegistry;
import io.fluxmq.serialization.FluxMqEnvelopeCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.SmartLifecycle;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

/** Managed JeroMQ SUB socket that dispatches messages to FluxMQ listeners. */
public final class ZmqFluxMqSubscriberContainer implements SmartLifecycle {
  private final ZContext context;
  private final FluxMqProperties properties;
  private final FluxMqListenerRegistry registry;
  private final FluxMqSerializer serializer;
  private final FluxMqEnvelopeCodec envelopeCodec;
  private final FluxMqErrorHandler errorHandler;
  private final FluxMqObservationHandler observationHandler;
  private final AtomicBoolean running = new AtomicBoolean();

  private volatile Thread receiveThread;

  /** Creates a managed subscriber container using one JeroMQ SUB socket. */
  public ZmqFluxMqSubscriberContainer(
      ZContext context,
      FluxMqProperties properties,
      FluxMqListenerRegistry registry,
      FluxMqSerializer serializer,
      FluxMqEnvelopeCodec envelopeCodec,
      FluxMqErrorHandler errorHandler) {
    this(
        context,
        properties,
        registry,
        serializer,
        envelopeCodec,
        errorHandler,
        new FluxMqObservationHandler() {});
  }

  /** Creates a managed subscriber container using one JeroMQ SUB socket. */
  public ZmqFluxMqSubscriberContainer(
      ZContext context,
      FluxMqProperties properties,
      FluxMqListenerRegistry registry,
      FluxMqSerializer serializer,
      FluxMqEnvelopeCodec envelopeCodec,
      FluxMqErrorHandler errorHandler,
      FluxMqObservationHandler observationHandler) {
    this.context = Objects.requireNonNull(context, "context");
    this.properties = Objects.requireNonNull(properties, "properties");
    this.registry = Objects.requireNonNull(registry, "registry");
    this.serializer = Objects.requireNonNull(serializer, "serializer");
    this.envelopeCodec = Objects.requireNonNull(envelopeCodec, "envelopeCodec");
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
    this.observationHandler = Objects.requireNonNull(observationHandler, "observationHandler");
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    Set<String> topics = subscribedTopics();
    if (topics.isEmpty() && !properties.getSubscriber().configured()) {
      running.set(false);
      return;
    }
    ResolvedEndpoint endpoint = properties.getSubscriber().requireEndpoint("subscriber");
    Thread thread = new Thread(() -> receiveLoop(endpoint, topics), "fluxmq-subscriber");
    thread.setDaemon(true);
    receiveThread = thread;
    thread.start();
  }

  @Override
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    Thread thread = receiveThread;
    if (thread != null) {
      thread.interrupt();
      join(thread, properties.getShutdown().getTimeout());
    }
  }

  @Override
  public void stop(Runnable callback) {
    try {
      stop();
    } finally {
      callback.run();
    }
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  private void receiveLoop(ResolvedEndpoint endpoint, Set<String> topics) {
    try (ZMQ.Socket socket = context.createSocket(SocketType.SUB)) {
      socket.setReceiveTimeOut(100);
      switch (endpoint.mode()) {
        case BIND -> socket.bind(endpoint.endpoint());
        case CONNECT -> socket.connect(endpoint.endpoint());
        default -> throw new IllegalStateException("Unsupported endpoint mode " + endpoint.mode());
      }
      for (String topic : topics) {
        socket.subscribe(topic.getBytes(StandardCharsets.UTF_8));
      }
      try {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
          byte[] topicBytes = socket.recv(0);
          if (topicBytes == null) {
            continue;
          }
          byte[] metadataBytes = socket.recv(0);
          byte[] payloadBytes = socket.recv(0);
          if (metadataBytes == null || payloadBytes == null) {
            continue;
          }
          dispatch(new String(topicBytes, StandardCharsets.UTF_8), metadataBytes, payloadBytes);
        }
      } catch (ZMQException exception) {
        if (running.get()) {
          throw exception;
        }
      }
    } finally {
      running.set(false);
    }
  }

  private Set<String> subscribedTopics() {
    LinkedHashSet<String> topics = new LinkedHashSet<>();
    for (String topic : properties.getSubscriber().getTopics()) {
      if (topic == null || topic.isBlank()) {
        throw new IllegalStateException("fluxmq.subscriber.topics must not contain blank topics");
      }
      topics.add(topic);
    }
    topics.addAll(registry.topics());
    return topics;
  }

  private void dispatch(String topic, byte[] metadataBytes, byte[] payloadBytes) {
    FluxMqEnvelope envelope = envelopeCodec.decode(topic, metadataBytes, payloadBytes);
    List<FluxMqListenerMethod> listeners = registry.listenersFor(topic);
    for (FluxMqListenerMethod listener : listeners) {
      try {
        Object payload = serializer.deserialize(payloadBytes, listener.payloadType());
        invokeWithRetry(listener, payload, envelope);
      } catch (Throwable error) {
        observationHandler.listenerFailed(
            envelope, listener.description(), 1, error, Duration.ZERO);
        errorHandler.handle(error, envelope);
      }
    }
  }

  private void invokeWithRetry(
      FluxMqListenerMethod listener, Object payload, FluxMqEnvelope envelope) {
    long startedAt = System.nanoTime();
    int maxAttempts = properties.getRetry().getMaxAttempts();
    Throwable lastError = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        listener.invoke(payload, envelope.headers());
        observationHandler.listenerSucceeded(
            envelope, listener.description(), attempt, elapsedSince(startedAt));
        return;
      } catch (Throwable error) {
        lastError = error;
        if (attempt < maxAttempts) {
          sleep(properties.getRetry().getDelay());
        }
      }
    }
    observationHandler.listenerFailed(
        envelope, listener.description(), maxAttempts, lastError, elapsedSince(startedAt));
    errorHandler.handle(lastError, envelope);
  }

  private static void sleep(Duration delay) {
    try {
      Thread.sleep(delay.toMillis());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting to retry FluxMQ listener", exception);
    }
  }

  private static void join(Thread thread, Duration timeout) {
    try {
      thread.join(timeout.toMillis());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private static Duration elapsedSince(long startedAt) {
    return Duration.ofNanos(System.nanoTime() - startedAt);
  }
}
