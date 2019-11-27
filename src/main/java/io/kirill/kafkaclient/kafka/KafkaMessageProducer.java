package io.kirill.kafkaclient.kafka;

import java.util.Properties;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;

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

  public void sendContinuously(Supplier<KeyValue<String, String>> messageSource, long delay) {
    new Thread(() -> {
      while (running) {
        try {
          var pair = messageSource.get();
          send(pair.key, pair.value);
          Thread.sleep(delay);
        } catch (Exception exception) {
          running = false;
        }
      }
    }).start();
  }

  public void send(String msg) {
    log.info("sending {}", msg);
    var record = new ProducerRecord<String, String>(topic, msg);
    producer.send(record, producerCallback);
  }

  public void send(String key, String msg) {
    log.info("sending {} - {}", key, msg);
    var record = new ProducerRecord<>(topic, key, msg);
    producer.send(record, producerCallback);
  }

  public void stop() {
    log.info("stopping kafka producer for topic {}", topic);
    running = false;
    producer.close();
  }
}
