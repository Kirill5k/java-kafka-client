package io.kirill.kafkaclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.function.Supplier;

@Slf4j
public class KafkaMessageProducer {
  private static Callback producerCallback =
      (meta, e) -> log.info("sent message to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());


  private final String topic;
  private final KafkaProducer<String, String> producer;

  private boolean running = true;

  public KafkaMessageProducer(Properties props, String topic) {
    this.topic = topic;
    this.producer = new KafkaProducer<>(props);
  }

  public void sendContinuously(Supplier<String> messageSource, long delay) {
    new Thread(() -> {
      while (running) {
        try {
          send(messageSource.get());
          Thread.sleep(delay);
        } catch (Exception exception) {
          running = false;
        }
      }
    }).start();
  }

  public void send(String msg) {
    log.info("sending message {}", msg);
    var record = new ProducerRecord<String, String>(topic, msg);
    producer.send(record, producerCallback);
  }

  public void send(String key, String msg) {
    log.info("sending message {} with key {}", msg, key);
    var record = new ProducerRecord<>(topic, key, msg);
    producer.send(record, producerCallback);
  }

  public void stop() {
    log.info("stopping kafka producer for topic {}", topic);
    running = false;
    producer.close();
  }
}
