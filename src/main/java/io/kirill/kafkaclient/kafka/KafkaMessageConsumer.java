package io.kirill.kafkaclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

@Slf4j
public class KafkaMessageConsumer {
  private final String topic;
  private final KafkaConsumer<String, String> consumer;

  public KafkaMessageConsumer(Properties props, String topic) {
    this.topic = topic;
    this.consumer = new KafkaConsumer<>(props);
  }

  public void onMessage(BiConsumer<String, String> messageConsumer) {
    consumer.subscribe(List.of(topic));
    var thread = new Thread(() -> {
      try {
        consumeMessages(messageConsumer);
      } catch (WakeupException exception) {
        log.info("received shutdown signal");
      } finally {
        consumer.close();
      }
    });
    thread.start();
  }

  private void consumeMessages(BiConsumer<String, String> messageConsumer) {
    while (true) {
      var records = consumer.poll(Duration.ofMillis(100));
      records.forEach(rec -> {
        log.info("received message from part {} with key {} and offset {}: {}", rec.partition(), rec.key(), rec.offset(), rec.value());
        messageConsumer.accept(generateId(rec), rec.value());
      });
    }
  }

  private String generateId(ConsumerRecord<String, String> record) {
    return String.format("%s-%s-%s", record.topic(), record.partition(), record.offset());
  }

  public void stop() {
    log.info("stopping kafka consumer");
    consumer.wakeup();
  }
}
