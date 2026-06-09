package io.fluxmq;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a Spring bean method as a FluxMQ topic listener. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FluxMqListener {

  /** Topic handled by the annotated method. */
  String value();
}
