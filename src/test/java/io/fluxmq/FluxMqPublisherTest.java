package io.fluxmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.Map;
import org.junit.jupiter.api.Test;

final class FluxMqPublisherTest {

  @Test
  void publishesTypedTopicPayload() {
    RecordingPublisher publisher = new RecordingPublisher();
    FluxMqTopic<TestEvent> topic = FluxMqTopic.of("events.test", TestEvent.class);
    TestEvent payload = new TestEvent("event-1");
    FluxMqHeaders headers = FluxMqHeaders.of(Map.of("tenant", "acme"));

    publisher.publish(topic, payload, headers);

    assertThat(publisher.topic).isEqualTo("events.test");
    assertThat(publisher.payload).isSameAs(payload);
    assertThat(publisher.headers).isSameAs(headers);
  }

  @Test
  void typedPublishRejectsNullPayload() {
    RecordingPublisher publisher = new RecordingPublisher();
    FluxMqTopic<TestEvent> topic = FluxMqTopic.of("events.test", TestEvent.class);

    assertThatNullPointerException().isThrownBy(() -> publisher.publish(topic, null));
  }

  private record TestEvent(String id) {}

  private static final class RecordingPublisher implements FluxMqPublisher {
    private String topic;
    private Object payload;
    private FluxMqHeaders headers;

    @Override
    public void publish(String topic, Object payload) {
      publish(topic, payload, FluxMqHeaders.empty());
    }

    @Override
    public void publish(String topic, Object payload, FluxMqHeaders headers) {
      this.topic = topic;
      this.payload = payload;
      this.headers = headers;
    }
  }
}
