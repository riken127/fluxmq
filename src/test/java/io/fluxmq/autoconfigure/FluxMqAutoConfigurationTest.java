package io.fluxmq.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.fluxmq.FluxMqPublisher;
import io.fluxmq.FluxMqSerializer;
import io.fluxmq.listener.FluxMqListenerRegistry;
import io.fluxmq.serialization.StringFluxMqSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

final class FluxMqAutoConfigurationTest {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(FluxMqAutoConfiguration.class));

  @Test
  void createsDefaultBeansWhenEnabled() {
    contextRunner
        .withPropertyValues("fluxmq.publisher.bind=inproc://auto-config")
        .run(
            context -> {
              assertThat(context).hasSingleBean(FluxMqPublisher.class);
              assertThat(context).hasSingleBean(FluxMqSerializer.class);
              assertThat(context).hasSingleBean(FluxMqListenerRegistry.class);
            });
  }

  @Test
  void backsOffWhenDisabled() {
    contextRunner
        .withPropertyValues("fluxmq.enabled=false")
        .run(context -> assertThat(context).doesNotHaveBean(FluxMqPublisher.class));
  }

  @Test
  void rejectsAmbiguousPublisherEndpoint() {
    contextRunner
        .withPropertyValues(
            "fluxmq.publisher.bind=inproc://one", "fluxmq.publisher.connect=inproc://two")
        .run(
            context ->
                assertThat(context.getStartupFailure())
                    .hasRootCauseMessage(
                        "fluxmq.publisher must not configure both bind and connect"));
  }

  @Test
  void selectsConfiguredSerializerFormat() {
    contextRunner
        .withPropertyValues(
            "fluxmq.publisher.bind=inproc://string-serializer",
            "fluxmq.serialization.format=string")
        .run(
            context ->
                assertThat(context.getBean(FluxMqSerializer.class))
                    .isInstanceOf(StringFluxMqSerializer.class));
  }
}
