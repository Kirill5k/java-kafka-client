package io.kirill.kafkaclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class KafkaMessageConsumer {
  private final String topic;
  private final KafkaConsumer<String, String> consumer;

  public KafkaMessageConsumer(Properties props, String topic) {
    this.topic = topic;
    this.consumer = new KafkaConsumer<>(props);
  }

  public void onMessage(Consumer<String> messageConsumer) {
    consumer.subscribe(List.of(topic));
    var thread = new Thread(() -> {
      try {
        while (true) {
          consumerMessage(messageConsumer);
        }
      } catch (WakeupException exception) {
        log.info("received shutdown signal");
      } finally {
        consumer.close();
      }
    });
    thread.start();
  }

  private void consumerMessage(Consumer<String> messageConsumer) {
    var records = consumer.poll(Duration.ofMillis(100));
    records.forEach(rec -> {
      log.info("received message from part {} with key {} and offset {}: {}", rec.partition(), rec.key(), rec.offset(), rec.value());
      messageConsumer.accept(rec.value());
    });
  }

  public void stop() {
    log.info("stopping kafka consumer");
    consumer.wakeup();
  }
}
