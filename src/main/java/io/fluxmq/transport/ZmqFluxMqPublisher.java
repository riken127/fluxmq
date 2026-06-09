package io.fluxmq.transport;

import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqHeaders;
import io.fluxmq.FluxMqObservationHandler;
import io.fluxmq.FluxMqPublisher;
import io.fluxmq.FluxMqSerializer;
import io.fluxmq.autoconfigure.FluxMqProperties.ResolvedEndpoint;
import io.fluxmq.serialization.FluxMqEnvelopeCodec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/** JeroMQ PUB socket implementation of {@link FluxMqPublisher}. */
public final class ZmqFluxMqPublisher implements FluxMqPublisher, AutoCloseable {
  private final FluxMqSerializer serializer;
  private final FluxMqEnvelopeCodec envelopeCodec;
  private final Clock clock;
  private final FluxMqObservationHandler observationHandler;
  private final BlockingQueue<SendCommand> queue;
  private final Duration enqueueTimeout;
  private final Duration shutdownTimeout;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CompletableFuture<Void> started = new CompletableFuture<>();
  private final Thread worker;

  /** Creates a publisher using one JeroMQ PUB socket. */
  public ZmqFluxMqPublisher(
      ZContext context,
      ResolvedEndpoint endpoint,
      FluxMqSerializer serializer,
      FluxMqEnvelopeCodec envelopeCodec,
      Clock clock) {
    this(
        context,
        endpoint,
        serializer,
        envelopeCodec,
        clock,
        new FluxMqObservationHandler() {},
        1024,
        Duration.ofSeconds(5),
        Duration.ofSeconds(5));
  }

  /** Creates a publisher using one JeroMQ PUB socket owned by a worker thread. */
  public ZmqFluxMqPublisher(
      ZContext context,
      ResolvedEndpoint endpoint,
      FluxMqSerializer serializer,
      FluxMqEnvelopeCodec envelopeCodec,
      Clock clock,
      FluxMqObservationHandler observationHandler,
      int queueCapacity,
      Duration enqueueTimeout,
      Duration shutdownTimeout) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(endpoint, "endpoint");
    this.serializer = Objects.requireNonNull(serializer, "serializer");
    this.envelopeCodec = Objects.requireNonNull(envelopeCodec, "envelopeCodec");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.observationHandler = Objects.requireNonNull(observationHandler, "observationHandler");
    this.queue = new ArrayBlockingQueue<>(requirePositive(queueCapacity, "queueCapacity"));
    this.enqueueTimeout = requireNotNegative(enqueueTimeout, "enqueueTimeout");
    this.shutdownTimeout = requireNotNegative(shutdownTimeout, "shutdownTimeout");
    this.worker = new Thread(() -> runWorker(context, endpoint), "fluxmq-publisher");
    this.worker.setDaemon(true);
    this.worker.start();
    awaitStarted();
  }

  @Override
  public void publish(String topic, Object payload) {
    publish(topic, payload, FluxMqHeaders.empty());
  }

  @Override
  public void publish(String topic, Object payload, FluxMqHeaders headers) {
    long startedAt = System.nanoTime();
    String resolvedTopic = requireText(topic, "topic");
    try {
      ensureOpen();
      Objects.requireNonNull(payload, "payload");
      FluxMqHeaders resolvedHeaders = Objects.requireNonNull(headers, "headers");
      byte[] payloadBytes = serializer.serialize(payload);
      FluxMqEnvelope envelope =
          new FluxMqEnvelope(
              UUID.randomUUID().toString(),
              resolvedTopic,
              payload.getClass().getName(),
              Instant.now(clock),
              resolvedHeaders,
              payloadBytes);
      SendCommand command =
          new SendCommand(
              resolvedTopic,
              envelopeCodec.encodeMetadata(envelope),
              payloadBytes,
              new CompletableFuture<>());

      enqueue(command);
      observationHandler.published(envelope, elapsedSince(startedAt));
    } catch (RuntimeException | Error error) {
      observationHandler.publishFailed(resolvedTopic, error, elapsedSince(startedAt));
      throw error;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    join(worker, shutdownTimeout);
    if (worker.isAlive()) {
      worker.interrupt();
      join(worker, shutdownTimeout);
    }
  }

  private void runWorker(ZContext context, ResolvedEndpoint endpoint) {
    try (ZMQ.Socket socket = context.createSocket(SocketType.PUB)) {
      switch (endpoint.mode()) {
        case BIND -> socket.bind(endpoint.endpoint());
        case CONNECT -> socket.connect(endpoint.endpoint());
        default -> throw new IllegalStateException("Unsupported endpoint mode " + endpoint.mode());
      }
      started.complete(null);
      while (!closed.get() || !queue.isEmpty()) {
        SendCommand command = queue.poll(100, TimeUnit.MILLISECONDS);
        if (command == null) {
          continue;
        }
        send(socket, command);
      }
    } catch (Throwable error) {
      started.completeExceptionally(error);
      failQueuedCommands(error);
    }
  }

  private void send(ZMQ.Socket socket, SendCommand command) {
    try {
      socket.sendMore(command.topic());
      socket.sendMore(command.metadata());
      socket.send(command.payload());
      command.completion().complete(null);
    } catch (RuntimeException | Error error) {
      command.completion().completeExceptionally(error);
    }
  }

  private void enqueue(SendCommand command) {
    try {
      if (!queue.offer(command, enqueueTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("FluxMQ publisher queue is full");
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while enqueueing FluxMQ publish command", exception);
    }
    try {
      command.completion().join();
    } catch (CompletionException exception) {
      throw rethrow(exception.getCause());
    }
  }

  private void awaitStarted() {
    try {
      started.join();
    } catch (CompletionException exception) {
      throw rethrow(exception.getCause());
    }
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("FluxMQ publisher is closed");
    }
  }

  private void failQueuedCommands(Throwable error) {
    SendCommand command;
    while ((command = queue.poll()) != null) {
      command.completion().completeExceptionally(error);
    }
  }

  private static RuntimeException rethrow(Throwable error) {
    if (error instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    if (error instanceof Error seriousError) {
      throw seriousError;
    }
    return new IllegalStateException(error);
  }

  private static void join(Thread thread, Duration timeout) {
    try {
      thread.join(timeout.toMillis());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static Duration requireNotNegative(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  private static Duration elapsedSince(long startedAt) {
    return Duration.ofNanos(System.nanoTime() - startedAt);
  }

  private record SendCommand(
      String topic, byte[] metadata, byte[] payload, CompletableFuture<Void> completion) {}
}
