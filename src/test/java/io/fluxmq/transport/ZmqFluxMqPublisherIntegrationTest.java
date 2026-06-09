package io.fluxmq.transport;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fluxmq.FluxMqEnvelope;
import io.fluxmq.FluxMqObservationHandler;
import io.fluxmq.autoconfigure.FluxMqProperties.EndpointMode;
import io.fluxmq.autoconfigure.FluxMqProperties.ResolvedEndpoint;
import io.fluxmq.serialization.FluxMqEnvelopeCodec;
import io.fluxmq.serialization.JacksonFluxMqSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

final class ZmqFluxMqPublisherIntegrationTest {

  @Test
  void sendsMultipartTopicMetadataAndPayload() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    FluxMqEnvelopeCodec codec = new FluxMqEnvelopeCodec(objectMapper);
    try (ZContext context = new ZContext();
        ZMQ.Socket subscriber = context.createSocket(SocketType.SUB)) {
      subscriber.bind("inproc://publisher-test");
      subscriber.subscribe("orders.created".getBytes(StandardCharsets.UTF_8));
      subscriber.setReceiveTimeOut(1_000);
      AtomicReference<String> observedTopic = new AtomicReference<>();

      try (ZmqFluxMqPublisher publisher =
          new ZmqFluxMqPublisher(
              context,
              new ResolvedEndpoint(EndpointMode.CONNECT, "inproc://publisher-test"),
              new JacksonFluxMqSerializer(objectMapper),
              codec,
              Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
              new FluxMqObservationHandler() {
                @Override
                public void published(FluxMqEnvelope envelope, Duration duration) {
                  observedTopic.set(envelope.topic());
                }
              },
              16,
              Duration.ofSeconds(1),
              Duration.ofSeconds(1))) {
        Thread.sleep(200);

        byte[] topicBytes = null;
        byte[] metadata = null;
        byte[] payload = null;
        for (int attempt = 0; attempt < 10 && topicBytes == null; attempt++) {
          publisher.publish("orders.created", new OrderCreated("order-1"));
          topicBytes = subscriber.recv(0);
          if (topicBytes != null) {
            metadata = subscriber.recv(0);
            payload = subscriber.recv(0);
          }
        }

        assertThat(topicBytes).isNotNull();
        assertThat(metadata).isNotNull();
        assertThat(payload).isNotNull();
        String topic = new String(topicBytes, StandardCharsets.UTF_8);
        FluxMqEnvelope envelope = codec.decode(topic, metadata, payload);

        assertThat(topic).isEqualTo("orders.created");
        assertThat(envelope.topic()).isEqualTo("orders.created");
        assertThat(envelope.timestamp()).isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
        assertThat(objectMapper.readValue(payload, OrderCreated.class))
            .isEqualTo(new OrderCreated("order-1"));
        assertThat(observedTopic).hasValue("orders.created");
      }
    }
  }

  record OrderCreated(String id) {}
}
