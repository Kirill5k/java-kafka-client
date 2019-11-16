package io.kirill.kafkaclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class KafkaMessageProducer {
  private static Callback producerCallback =
      (meta, e) -> log.info("sent message to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());


  private final String topic;
  private final KafkaProducer<String, String> producer;

  public KafkaMessageProducer(Properties props, String topic) {
    this.topic = topic;
    this.producer = new KafkaProducer<>(props);
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
    producer.close();
  }
}
