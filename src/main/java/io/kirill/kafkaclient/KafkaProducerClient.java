package io.kirill.kafkaclient;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static io.kirill.kafkaclient.KafkaConfig.MY_TOPIC;
import static io.kirill.kafkaclient.KafkaConfig.defaultProducerProps;

@Slf4j
public class KafkaProducerClient {
  private static Callback producerCallback =
      (meta, e) -> log.info("sent message to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());

  public static void main(String[] args) {
    var record = new ProducerRecord<String, String>(MY_TOPIC, "hello, world");
    var producer = new KafkaProducer<String, String>(defaultProducerProps());
    producer.send(record, producerCallback);

    var key = "record-key";
    var recordWithKey = new ProducerRecord<String, String>(MY_TOPIC, key, "Hello, world, with key");
    producer.send(recordWithKey, producerCallback);

    producer.close();
  }
}
