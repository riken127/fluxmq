package io.fluxmq.transport;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqErrorHandler;
import io.fluxmq.FluxMqHeaders;
import io.fluxmq.FluxMqObservationHandler;
import io.fluxmq.autoconfigure.FluxMqProperties;
import io.fluxmq.listener.FluxMqListenerMethod;
import io.fluxmq.listener.FluxMqListenerRegistry;
import io.fluxmq.serialization.FluxMqEnvelopeCodec;
import io.fluxmq.serialization.JacksonFluxMqSerializer;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

final class ZmqFluxMqSubscriberContainerIntegrationTest {

  @Test
  void dispatchesMessageToMatchingListener() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    FluxMqEnvelopeCodec codec = new FluxMqEnvelopeCodec(objectMapper);
    JacksonFluxMqSerializer serializer = new JacksonFluxMqSerializer(objectMapper);
    Listener listenerBean = new Listener();
    FluxMqListenerRegistry registry = registryFor(listenerBean, "handle");
    FluxMqProperties properties = subscriberProperties("inproc://subscriber-dispatch");

    try (ZContext context = new ZContext();
        ZMQ.Socket publisher = context.createSocket(SocketType.PUB)) {
      publisher.bind("inproc://subscriber-dispatch");
      ZmqFluxMqSubscriberContainer container =
          new ZmqFluxMqSubscriberContainer(
              context, properties, registry, serializer, codec, (error, envelope) -> {});
      container.start();
      Thread.sleep(150);

      send(publisher, codec, serializer, "orders.created", new OrderCreated("order-1"));

      assertThat(await(listenerBean.received)).isEqualTo(new OrderCreated("order-1"));
      container.stop();
      assertThat(container.isRunning()).isFalse();
    }
  }

  @Test
  void sendsListenerFailuresToErrorHandler() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    FluxMqEnvelopeCodec codec = new FluxMqEnvelopeCodec(objectMapper);
    JacksonFluxMqSerializer serializer = new JacksonFluxMqSerializer(objectMapper);
    ThrowingListener listenerBean = new ThrowingListener();
    FluxMqListenerRegistry registry = registryFor(listenerBean, "handle");
    FluxMqProperties properties = subscriberProperties("inproc://subscriber-error");
    properties.getRetry().setMaxAttempts(2);
    AtomicReference<String> failedTopic = new AtomicReference<>();
    FluxMqErrorHandler errorHandler = (error, envelope) -> failedTopic.set(envelope.topic());
    AtomicReference<Integer> observedAttempts = new AtomicReference<>();
    FluxMqObservationHandler observationHandler =
        new FluxMqObservationHandler() {
          @Override
          public void listenerFailed(
              FluxMqEnvelope envelope,
              String listener,
              int attempts,
              Throwable error,
              java.time.Duration duration) {
            observedAttempts.set(attempts);
          }
        };

    try (ZContext context = new ZContext();
        ZMQ.Socket publisher = context.createSocket(SocketType.PUB)) {
      publisher.bind("inproc://subscriber-error");
      ZmqFluxMqSubscriberContainer container =
          new ZmqFluxMqSubscriberContainer(
              context, properties, registry, serializer, codec, errorHandler, observationHandler);
      container.start();
      Thread.sleep(150);

      send(publisher, codec, serializer, "orders.created", new OrderCreated("order-1"));

      assertThat(await(failedTopic)).isEqualTo("orders.created");
      assertThat(observedAttempts).hasValue(2);
      assertThat(listenerBean.attempts).hasValue(2);
      container.stop();
    }
  }

  private static FluxMqListenerRegistry registryFor(Object bean, String methodName)
      throws ReflectiveOperationException {
    Method method = bean.getClass().getMethod(methodName, OrderCreated.class);
    FluxMqListenerRegistry registry = new FluxMqListenerRegistry();
    registry.register(FluxMqListenerMethod.create(bean, method, "orders.created"));
    return registry;
  }

  private static FluxMqProperties subscriberProperties(String endpoint) {
    FluxMqProperties properties = new FluxMqProperties();
    properties.getPublisher().setBind("inproc://unused");
    properties.getSubscriber().setConnect(endpoint);
    return properties;
  }

  private static void send(
      ZMQ.Socket publisher,
      FluxMqEnvelopeCodec codec,
      JacksonFluxMqSerializer serializer,
      String topic,
      OrderCreated payload) {
    byte[] payloadBytes = serializer.serialize(payload);
    FluxMqEnvelope envelope =
        new FluxMqEnvelope(
            "event-1",
            topic,
            OrderCreated.class.getName(),
            Instant.parse("2026-01-01T00:00:00Z"),
            FluxMqHeaders.empty(),
            payloadBytes);
    publisher.sendMore(topic);
    publisher.sendMore(codec.encodeMetadata(envelope));
    publisher.send(payloadBytes);
  }

  private static <T> T await(AtomicReference<T> reference) throws InterruptedException {
    long deadline = System.nanoTime() + 2_000_000_000L;
    T value;
    while ((value = reference.get()) == null && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    return value;
  }

  public record OrderCreated(String id) {}

  public static final class Listener {
    final AtomicReference<OrderCreated> received = new AtomicReference<>();

    public void handle(OrderCreated event) {
      received.set(event);
    }
  }

  public static final class ThrowingListener {
    final AtomicReference<Integer> attempts = new AtomicReference<>(0);

    public void handle(OrderCreated event) {
      attempts.updateAndGet(value -> value + 1);
      throw new IllegalStateException("boom");
    }
  }
}
