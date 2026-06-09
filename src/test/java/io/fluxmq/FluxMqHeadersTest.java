package io.fluxmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FluxMqHeadersTest {

  @Test
  void copiesInputMap() {
    LinkedHashMap<String, String> source = new LinkedHashMap<>();
    source.put("tenant", "acme");

    FluxMqHeaders headers = FluxMqHeaders.of(source);
    source.put("tenant", "other");

    assertThat(headers.get("tenant")).contains("acme");
    assertThat(headers.asMap()).containsExactly(Map.entry("tenant", "acme"));
  }

  @Test
  void rejectsBlankKeys() {
    assertThatThrownBy(() -> FluxMqHeaders.of(Map.of(" ", "value")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("header key");
  }
}
