package io.fluxmq.example;

import io.fluxmq.FluxMqPublisher;
import java.util.UUID;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BasicPubSubApplication {

  public static void main(String[] args) {
    SpringApplication.run(BasicPubSubApplication.class, args);
  }

  @Bean
  ApplicationRunner publishExample(OrderService orderService) {
    return ignored -> {
      Thread.sleep(500);
      orderService.createOrder(UUID.randomUUID());
    };
  }

  @Bean
  OrderService orderService(FluxMqPublisher publisher) {
    return new OrderService(publisher);
  }
}
