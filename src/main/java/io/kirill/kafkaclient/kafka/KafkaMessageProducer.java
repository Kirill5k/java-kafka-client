package io.kirill.kafkaclient.kafka;

import java.util.Properties;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;

@Slf4j
public class KafkaMessageProducer<K, V> {
  private static Callback producerCallback =
      (meta, e) -> log.info("sent message to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());

  private final String topic;
  private final KafkaProducer<K, V> producer;

  private boolean running = true;

  private KafkaMessageProducer(Properties props, String topic, Serializer<K> keySer, Serializer<V> valueSer) {
    this.topic = topic;
    this.producer = new KafkaProducer<>(props, keySer, valueSer);
  }

  public static KafkaMessageProducer<String, String> to(String topic, Properties props) {
    return new KafkaMessageProducer<>(props, topic, new StringSerializer(), new StringSerializer());
  }

  public static <K, V> KafkaMessageProducer<K, V> to(String topic, Properties props, Serializer<K> keySer, Serializer<V> valueSer) {
    return new KafkaMessageProducer<>(props, topic, keySer, valueSer);
  }

  public void sendContinuously(Supplier<KeyValue<K, V>> messageSource, long delay) {
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

  public void send(V msg) {
    log.info("sending {}", msg);
    var record = new ProducerRecord<K, V>(topic, msg);
    producer.send(record, producerCallback);
  }

  public void send(KeyValue<K, V> keyValue) {
    send(keyValue.key, keyValue.value);
  }

  public void send(K key, V msg) {
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
