package io.fluxmq.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fluxmq.FluxMqErrorHandler;
import io.fluxmq.FluxMqObservationHandler;
import io.fluxmq.FluxMqPublisher;
import io.fluxmq.FluxMqSerializer;
import io.fluxmq.error.LoggingFluxMqErrorHandler;
import io.fluxmq.listener.FluxMqListenerBeanPostProcessor;
import io.fluxmq.listener.FluxMqListenerRegistry;
import io.fluxmq.serialization.ByteArrayFluxMqSerializer;
import io.fluxmq.serialization.FluxMqEnvelopeCodec;
import io.fluxmq.serialization.FluxMqSerializerRegistry;
import io.fluxmq.serialization.JacksonFluxMqSerializer;
import io.fluxmq.serialization.StringFluxMqSerializer;
import io.fluxmq.transport.ZmqFluxMqPublisher;
import io.fluxmq.transport.ZmqFluxMqSubscriberContainer;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.zeromq.ZContext;

/** Spring Boot auto-configuration for FluxMQ. */
@AutoConfiguration
@EnableConfigurationProperties(FluxMqProperties.class)
@ConditionalOnProperty(
    prefix = "fluxmq",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class FluxMqAutoConfiguration {

  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean
  ZContext fluxMqContext() {
    return new ZContext();
  }

  @Bean
  @ConditionalOnMissingBean(name = "fluxMqObjectMapper")
  ObjectMapper fluxMqObjectMapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqSerializerRegistry fluxMqSerializerRegistry(ObjectMapper fluxMqObjectMapper) {
    return FluxMqSerializerRegistry.builder()
        .serializer("json", new JacksonFluxMqSerializer(fluxMqObjectMapper))
        .serializer("string", new StringFluxMqSerializer())
        .serializer("bytes", new ByteArrayFluxMqSerializer())
        .build();
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqSerializer fluxMqSerializer(
      FluxMqSerializerRegistry serializerRegistry, FluxMqProperties properties) {
    return serializerRegistry.serializer(properties.getSerialization().getFormat());
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqErrorHandler fluxMqErrorHandler() {
    return new LoggingFluxMqErrorHandler();
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqObservationHandler fluxMqObservationHandler() {
    return new FluxMqObservationHandler() {};
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqEnvelopeCodec fluxMqEnvelopeCodec(ObjectMapper fluxMqObjectMapper) {
    return new FluxMqEnvelopeCodec(fluxMqObjectMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqListenerRegistry fluxMqListenerRegistry() {
    return new FluxMqListenerRegistry();
  }

  @Bean
  @ConditionalOnMissingBean
  FluxMqListenerBeanPostProcessor fluxMqListenerBeanPostProcessor(FluxMqListenerRegistry registry) {
    return new FluxMqListenerBeanPostProcessor(registry);
  }

  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean
  FluxMqPublisher fluxMqPublisher(
      ZContext context,
      FluxMqSerializer serializer,
      FluxMqEnvelopeCodec envelopeCodec,
      FluxMqProperties properties,
      FluxMqObservationHandler observationHandler) {
    return new ZmqFluxMqPublisher(
        context,
        properties.getPublisher().requireEndpoint("publisher"),
        serializer,
        envelopeCodec,
        Clock.systemUTC(),
        observationHandler,
        properties.getPublisher().getQueueCapacity(),
        properties.getPublisher().getEnqueueTimeout(),
        properties.getShutdown().getTimeout());
  }

  @Bean
  @ConditionalOnMissingBean
  ZmqFluxMqSubscriberContainer fluxMqSubscriberContainer(
      ZContext context,
      FluxMqProperties properties,
      FluxMqListenerRegistry registry,
      FluxMqSerializer serializer,
      FluxMqEnvelopeCodec envelopeCodec,
      FluxMqErrorHandler errorHandler,
      FluxMqObservationHandler observationHandler) {
    return new ZmqFluxMqSubscriberContainer(
        context, properties, registry, serializer, envelopeCodec, errorHandler, observationHandler);
  }
}
