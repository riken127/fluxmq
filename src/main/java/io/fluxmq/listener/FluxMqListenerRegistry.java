package io.fluxmq.listener;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Registry of topic-to-listener mappings discovered from Spring beans. */
public final class FluxMqListenerRegistry {
  private final Map<String, List<FluxMqListenerMethod>> listenersByTopic = new LinkedHashMap<>();

  /** Registers a validated listener method. */
  public synchronized void register(FluxMqListenerMethod listenerMethod) {
    listenersByTopic
        .computeIfAbsent(listenerMethod.topic(), ignored -> new ArrayList<>())
        .add(listenerMethod);
  }

  /** Returns listeners registered for a topic. */
  public synchronized List<FluxMqListenerMethod> listenersFor(String topic) {
    return List.copyOf(listenersByTopic.getOrDefault(topic, List.of()));
  }

  /** Returns discovered listener topics in registration order. */
  public synchronized Set<String> topics() {
    return new LinkedHashSet<>(listenersByTopic.keySet());
  }

  /** Returns true when no listener methods are registered. */
  public synchronized boolean isEmpty() {
    return listenersByTopic.isEmpty();
  }
}
