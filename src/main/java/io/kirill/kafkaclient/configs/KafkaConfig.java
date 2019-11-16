package io.kirill.kafkaclient.configs;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaConfig {
  public static final String MY_TOPIC = "test-topic.v1";
  public static final String GROUP_ID = "my-group";

  private static final String SERVER = "127.0.0.1:9092";

  public static Properties defaultProducerProps() {
    var props = new Properties();
    props.setProperty(BOOTSTRAP_SERVERS_CONFIG, SERVER);
    props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }

  public static Properties defaultConsumerProps() {
    var props = new Properties();
    props.setProperty(BOOTSTRAP_SERVERS_CONFIG, SERVER);
    props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }
}
