package io.fluxmq.listener;

import io.fluxmq.FluxMqHeaders;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/** Validated FluxMQ listener method and invocation metadata. */
public final class FluxMqListenerMethod {
  private final Object bean;
  private final Method method;
  private final String topic;
  private final Class<?> payloadType;
  private final boolean headersParameter;

  private FluxMqListenerMethod(
      Object bean, Method method, String topic, Class<?> payloadType, boolean headersParameter) {
    this.bean = Objects.requireNonNull(bean, "bean");
    this.method = Objects.requireNonNull(method, "method");
    this.topic = requireText(topic, "topic");
    this.payloadType = Objects.requireNonNull(payloadType, "payloadType");
    this.headersParameter = headersParameter;
    this.method.setAccessible(true);
  }

  /** Validates and creates listener method metadata. */
  public static FluxMqListenerMethod create(Object bean, Method method, String topic) {
    Objects.requireNonNull(method, "method");
    Class<?>[] parameterTypes = method.getParameterTypes();
    if (parameterTypes.length != 1 && parameterTypes.length != 2) {
      throw invalid(method, "must declare one payload parameter or payload plus FluxMqHeaders");
    }
    if (parameterTypes.length == 2 && !FluxMqHeaders.class.equals(parameterTypes[1])) {
      throw invalid(method, "second parameter must be FluxMqHeaders");
    }
    if (!Void.TYPE.equals(method.getReturnType())) {
      throw invalid(method, "must return void");
    }
    return new FluxMqListenerMethod(
        bean, method, topic, parameterTypes[0], parameterTypes.length == 2);
  }

  /** Returns the listener topic. */
  public String topic() {
    return topic;
  }

  /** Returns the payload type declared by the listener method. */
  public Class<?> payloadType() {
    return payloadType;
  }

  /** Returns a stable listener description for diagnostics and observations. */
  public String description() {
    return method.getDeclaringClass().getName() + "#" + method.getName();
  }

  /** Invokes the listener with the deserialized payload and optional headers. */
  public void invoke(Object payload, FluxMqHeaders headers) {
    try {
      if (headersParameter) {
        method.invoke(bean, payload, headers);
      } else {
        method.invoke(bean, payload);
      }
    } catch (IllegalAccessException exception) {
      throw new IllegalStateException("Could not invoke FluxMQ listener " + method, exception);
    } catch (InvocationTargetException exception) {
      Throwable target = exception.getTargetException();
      if (target instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (target instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("FluxMQ listener threw checked exception " + method, target);
    }
  }

  private static IllegalStateException invalid(Method method, String reason) {
    return new IllegalStateException("Invalid @FluxMqListener method " + method + ": " + reason);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
