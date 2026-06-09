package io.fluxmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

final class FluxMqTopicTest {

  @Test
  void createsTopic() {
    FluxMqTopic<TestEvent> topic = FluxMqTopic.of("events.test", TestEvent.class);

    assertThat(topic.name()).isEqualTo("events.test");
    assertThat(topic.payloadType()).isEqualTo(TestEvent.class);
  }

  @Test
  void rejectsBlankName() {
    assertThatThrownBy(() -> FluxMqTopic.of(" ", TestEvent.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("name");
  }

  @Test
  void rejectsNullPayloadType() {
    assertThatNullPointerException().isThrownBy(() -> FluxMqTopic.of("events.test", null));
  }

  private record TestEvent(String id) {}
}
