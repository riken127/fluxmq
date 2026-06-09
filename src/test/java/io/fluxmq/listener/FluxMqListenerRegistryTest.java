package io.fluxmq.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.fluxmq.FluxMqHeaders;
import io.fluxmq.FluxMqListener;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class FluxMqListenerRegistryTest {

  @Test
  void registersAnnotatedListenerMethods() {
    FluxMqListenerRegistry registry = new FluxMqListenerRegistry();
    FluxMqListenerBeanPostProcessor processor = new FluxMqListenerBeanPostProcessor(registry);

    processor.postProcessAfterInitialization(new ValidListener(), "validListener");

    assertThat(registry.topics()).containsExactly("orders.created");
    assertThat(registry.listenersFor("orders.created")).hasSize(1);
  }

  @Test
  void invokesListenerWithHeaders() throws ReflectiveOperationException {
    HeaderListener bean = new HeaderListener();
    Method method =
        HeaderListener.class.getMethod("handle", OrderCreated.class, FluxMqHeaders.class);
    FluxMqListenerMethod listener = FluxMqListenerMethod.create(bean, method, "orders.created");

    FluxMqHeaders headers = FluxMqHeaders.builder().correlationId("corr-1").build();
    listener.invoke(new OrderCreated("order-1"), headers);

    assertThat(bean.lastOrder.get()).isEqualTo(new OrderCreated("order-1"));
    assertThat(bean.lastHeaders.get()).isEqualTo(headers);
  }

  @Test
  void rejectsInvalidSecondParameter() throws ReflectiveOperationException {
    Method method = InvalidListener.class.getMethod("handle", OrderCreated.class, String.class);

    assertThatThrownBy(
            () -> FluxMqListenerMethod.create(new InvalidListener(), method, "orders.created"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("second parameter must be FluxMqHeaders");
  }

  public record OrderCreated(String id) {}

  public static final class ValidListener {
    @FluxMqListener("orders.created")
    public void handle(OrderCreated event) {}
  }

  public static final class HeaderListener {
    final AtomicReference<OrderCreated> lastOrder = new AtomicReference<>();
    final AtomicReference<FluxMqHeaders> lastHeaders = new AtomicReference<>();

    public void handle(OrderCreated event, FluxMqHeaders headers) {
      lastOrder.set(event);
      lastHeaders.set(headers);
    }
  }

  public static final class InvalidListener {
    public void handle(OrderCreated event, String headers) {}
  }
}
