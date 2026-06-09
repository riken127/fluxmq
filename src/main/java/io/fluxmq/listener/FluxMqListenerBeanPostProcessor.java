package io.fluxmq.listener;

import io.fluxmq.FluxMqListener;
import java.lang.reflect.Method;
import java.util.Objects;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotatedElementUtils;

/** Discovers {@link FluxMqListener} methods on Spring beans. */
public final class FluxMqListenerBeanPostProcessor implements BeanPostProcessor {
  private final FluxMqListenerRegistry registry;

  /** Creates a bean post-processor that registers discovered listeners. */
  public FluxMqListenerBeanPostProcessor(FluxMqListenerRegistry registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
    for (Method method : bean.getClass().getMethods()) {
      FluxMqListener annotation =
          AnnotatedElementUtils.findMergedAnnotation(method, FluxMqListener.class);
      if (annotation != null) {
        registry.register(FluxMqListenerMethod.create(bean, method, annotation.value()));
      }
    }
    return bean;
  }
}
